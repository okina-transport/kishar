package org.entur.kishar.gtfsrt;

import com.google.common.collect.Maps;
import com.google.protobuf.Duration;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.transit.realtime.GtfsRealtime;
import org.entur.kishar.gtfsrt.domain.CompositeKey;
import org.entur.kishar.gtfsrt.domain.GtfsRtData;
import org.entur.kishar.utils.BlobStoreService;
import org.redisson.Redisson;

import org.redisson.api.RMapCache;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.ByteArrayCodec;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;

import java.io.*;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Service
@Configuration
public class RedisService {


    enum Type {
        VEHICLE_POSITION("vehiclePositionMap"),
        TRIP_UPDATE("tripUpdateMap"),
        ALERT("alertMap"),
        ID_MAPPING("idMap");

        private String mapIdentifier;

        Type(String mapIdentifier) {
            this.mapIdentifier = mapIdentifier;
        }

        public String getMapIdentifier() {
            return mapIdentifier;
        }
    }

    private static Logger LOG = LoggerFactory.getLogger(RedisService.class);

    @Value("${kishar.mapping.stopplaces.update.frequency.min:60}")
    private int updateFrequency = 60;

    @Value("${kishar.mapping.quays.gcs.path}")
    private String quayMappingPath;

    @Value("${kishar.mapping.stopplaces.gcs.path}")
    private String stopPlaceMappingPath;

    @Autowired
    BlobStoreService blobStoreService;

    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    private final boolean redisEnabled;

    RedissonClient redisson;

    public RedisService(@Value("${kishar.redis.enabled:false}") boolean redisEnabled, @Value("${kishar.redis.host:}") String host, @Value("${kishar.redis.port:}") String port) {
        this.redisEnabled = redisEnabled;

        if (redisEnabled) {
            LOG.info("redis url = " + host + ":" + port);
            Config config = new Config();
            config.useReplicatedServers()
                    .addNodeAddress("redis://" + host + ":" + port);

            redisson = Redisson.create(config);

            executor.scheduleAtFixedRate(this::updateIdMapping, 1, updateFrequency, TimeUnit.MINUTES);        }
    }

    public void resetAllData() {
        LOG.info("Before - VEHICLE_POSITION: " + redisson.getMap(Type.VEHICLE_POSITION.mapIdentifier).size());
        redisson.getMap(Type.VEHICLE_POSITION.mapIdentifier).clear();
        LOG.info("After - VEHICLE_POSITION: " + redisson.getMap(Type.VEHICLE_POSITION.mapIdentifier).size());

        LOG.info("Before - TRIP_UPDATE: " + redisson.getMap(Type.TRIP_UPDATE.mapIdentifier).size());
        redisson.getMap(Type.TRIP_UPDATE.mapIdentifier).clear();
        LOG.info("After - TRIP_UPDATE: " + redisson.getMap(Type.TRIP_UPDATE.mapIdentifier).size());

        LOG.info("Before - ALERT: " + redisson.getMap(Type.ALERT.mapIdentifier).size());
        redisson.getMap(Type.ALERT.mapIdentifier).clear();
        LOG.info("After - ALERT: " + redisson.getMap(Type.ALERT.mapIdentifier).size());

        LOG.info("Before - ID_MAPPING: " + redisson.getMap(Type.ID_MAPPING.mapIdentifier).size());
        redisson.getMap(Type.ID_MAPPING.mapIdentifier).clear();
        LOG.info("After - ID_MAPPING: " + redisson.getMap(Type.ID_MAPPING.mapIdentifier).size());
    }

    private void updateIdMapping() {
        idMapping(quayMappingPath);
        idMapping(stopPlaceMappingPath);
    }

    private void idMapping(String csvFilePath){

        final InputStream blob = blobStoreService.getBlob(csvFilePath);

        Map<String, String> stopPlaceMappings = new HashMap<>();

        if (blob != null) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(blob));
            reader.lines().forEach(line -> {
                StringTokenizer tokenizer = new StringTokenizer(line, ",");
                String id = tokenizer.nextToken();
                String generatedId = tokenizer.nextToken();

                stopPlaceMappings.put(id, generatedId);
            });
        }
        writeIdMapping(stopPlaceMappings, Type.ID_MAPPING);
    }

    public void writeGtfsRt(Map<String, GtfsRtData> gtfsRt, Type type) {
        if (redisEnabled) {
            RMapCache<byte[], byte[]> gtfsRtMap = redisson.getMapCache(type.getMapIdentifier(), ByteArrayCodec.INSTANCE);
            for (String key : gtfsRt.keySet()) {
                GtfsRtData gtfsRtData = gtfsRt.get(key);
                long timeToLive = gtfsRtData.getTimeToLive().getSeconds();
                if (timeToLive > 0) {
                    if (Type.TRIP_UPDATE.equals(type)) {
                        mergeTripUpdatesAndsave(key.getBytes(), gtfsRtData.getData(), timeToLive);
                    } else {
                        gtfsRtMap.put(key.getBytes(), gtfsRtData.getData(), timeToLive, TimeUnit.SECONDS);
                    }

                }
            }
        }
    }

    public void writeIdMapping(Map<String, String> idMapping, Type type) {
        if (redisEnabled) {
            RMapCache<String, String> idMap = redisson.getMapCache(type.getMapIdentifier(), StringCodec.INSTANCE);
            idMap.putAll(idMapping);
        }
    }

    private void mergeTripUpdatesAndsave(byte[] key, byte[] gtfsRtDataBytes, long timeToLive) {
        RMapCache<byte[], byte[]> gtfsRtMap = redisson.getMapCache(Type.TRIP_UPDATE.getMapIdentifier(), ByteArrayCodec.INSTANCE);

        try {

            byte[] existingJourney = gtfsRtMap.get(key);
            if (existingJourney == null) {
                //no existing journey. puting the new one in cache
                gtfsRtMap.put(key, gtfsRtDataBytes, timeToLive, TimeUnit.SECONDS);
            } else {

                GtfsRealtime.FeedEntity entity = GtfsRealtime.FeedEntity.parseFrom(existingJourney);
                GtfsRealtime.FeedEntity incomingTripUpdate = GtfsRealtime.FeedEntity.parseFrom(gtfsRtDataBytes);
                GtfsRealtime.FeedEntity.Builder mergedEntity = GtfsRealtime.FeedEntity.newBuilder();

                mergedEntity.setTripUpdate(buildMergedTripUpdate(entity, incomingTripUpdate));
                mergedEntity.setVehicle(entity.getVehicle());
                mergedEntity.setId(entity.getId());


                Duration timeToLiveDur = Duration.newBuilder().setSeconds(timeToLive).build();
                GtfsRtData mergedGtfsData = new GtfsRtData(mergedEntity.build().toByteArray(), timeToLiveDur);
                gtfsRtMap.put(key, mergedGtfsData.getData() , timeToLive, TimeUnit.SECONDS);
            }

        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }

    }

    public GtfsRealtime.TripUpdate buildMergedTripUpdate(GtfsRealtime.FeedEntity existingEntity, GtfsRealtime.FeedEntity incomingEntity) {

        GtfsRealtime.TripUpdate.Builder mergedTripUpdate = GtfsRealtime.TripUpdate.newBuilder();
        mergedTripUpdate.setTrip(existingEntity.getTripUpdate().getTrip());
        mergedTripUpdate.setVehicle(existingEntity.getTripUpdate().getVehicle());

        if (!existingEntity.getTripUpdate().getTrip().getTripId().equals(incomingEntity.getTripUpdate().getTrip().getTripId())){
            LOG.error("===>merging different trips - " + existingEntity.getTripUpdate().getTrip().getTripId() + " - " + incomingEntity.getTripUpdate().getTrip().getTripId());
        }

        if (!existingEntity.getTripUpdate().getTrip().getRouteId().equals(incomingEntity.getTripUpdate().getTrip().getRouteId())){
            LOG.error("===>merging different trips - " + existingEntity.getTripUpdate().getTrip().getRouteId() + " - " + incomingEntity.getTripUpdate().getTrip().getRouteId());
        }

        if (!existingEntity.getTripUpdate().getVehicle().getId().equals(incomingEntity.getTripUpdate().getVehicle().getId())){
            LOG.error("===>merging different trips - " + existingEntity.getTripUpdate().getVehicle().getId() + " - " + incomingEntity.getTripUpdate().getVehicle().getId());
        }

        List<String> alreadySeenStops = new ArrayList<>();
        addStopUpdateTimes(mergedTripUpdate, alreadySeenStops, existingEntity.getTripUpdate().getStopTimeUpdateList());
        addStopUpdateTimes(mergedTripUpdate, alreadySeenStops, incomingEntity.getTripUpdate().getStopTimeUpdateList());

        return mergedTripUpdate.build();
    }

    private void addStopUpdateTimes(GtfsRealtime.TripUpdate.Builder mergedTripUpdate, List<String> alreadySeenStops, List<GtfsRealtime.TripUpdate.StopTimeUpdate> stopTimeUpdates){
        for (GtfsRealtime.TripUpdate.StopTimeUpdate stopTimeUpdate : stopTimeUpdates) {
            if (!alreadySeenStops.contains(stopTimeUpdate.getStopId())){
                mergedTripUpdate.addStopTimeUpdate(stopTimeUpdate);
                alreadySeenStops.add(stopTimeUpdate.getStopId());
            }
        }
    }

    public Map<String, byte[]> readGtfsRtMap(Type type) {
        if (redisEnabled) {
            RMapCache<byte[], byte[]> gtfsRtMap = redisson.getMapCache(type.getMapIdentifier(), ByteArrayCodec.INSTANCE);

            Map<String, byte[]> result = new HashMap<>();

            final Set<Map.Entry<byte[], byte[]>> entries = gtfsRtMap.readAllEntrySet();
            for (Map.Entry<byte[], byte[]> entry : entries) {
                final CompositeKey key = CompositeKey.reCreate(entry.getKey());
                if (key != null) {
                    result.put(key.asString(), entry.getValue());
                }
            }

            return result;
        } else {
            return Maps.newHashMap();
        }
    }

    public String readIdMap(Type type, String key) {
        if (redisEnabled) {
            RMapCache<String, String> idMap = redisson.getMapCache(type.getMapIdentifier(), StringCodec.INSTANCE);

            return idMap.get(key);
        } else {
            return null;
        }
    }
}
