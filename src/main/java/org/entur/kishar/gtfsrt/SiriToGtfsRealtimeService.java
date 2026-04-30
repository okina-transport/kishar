/*
 * Licensed under the EUPL, Version 1.2 or – as soon they will be approved by
 * the European Commission - subsequent versions of the EUPL (the "Licence");
 * You may not use this work except in compliance with the Licence.
 * You may obtain a copy of the Licence at:
 *
 *   https://joinup.ec.europa.eu/software/page/eupl
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the Licence is distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Licence for the specific language governing permissions and
 * limitations under the Licence.
 */
package org.entur.kishar.gtfsrt;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.protobuf.Duration;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import com.google.transit.realtime.GtfsRealtime.*;
import org.apache.commons.collections4.CollectionUtils;
import org.entur.kishar.gtfsrt.domain.CompositeKey;
import org.entur.kishar.gtfsrt.domain.GtfsRtData;
import org.entur.kishar.gtfsrt.helpers.SiriLibrary;
import org.entur.kishar.gtfsrt.mappers.GtfsRtMapper;
import org.entur.kishar.gtfsrt.mappers.IdMapper;
import org.entur.kishar.metrics.PrometheusMetricsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;
import uk.org.siri.www.siri.*;
import org.entur.kishar.utils.StopTimeUpdateComparator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.entur.kishar.gtfsrt.helpers.GtfsRealtimeLibrary.createFeedMessageBuilder;
import static org.entur.kishar.utils.Constants.MAX_END_DATE;

@Service
@Configuration
public class SiriToGtfsRealtimeService {
    private static final Logger LOG = LoggerFactory.getLogger(SiriToGtfsRealtimeService.class);

    private static final String MEDIA_TYPE_APPLICATION_JSON = "application/json";

    /**
     * Time, in seconds, after which a vehicle update is considered stale
     */
    private static final int GRACE_PERIOD = 5 * 60;

    private final AlertFactory alertFactory;
    private final IdMapper idMapper;
    private final GtfsRtMapper gtfsRtMapper;
    private final PrometheusMetricsService prometheusMetricsService;
    private final RedisService redisService;
    private final Map<String, FeedMessage> tripUpdatesByDatasetId = new ConcurrentHashMap<>();
    private final Map<String, FeedMessage> vehiclePositionsByDatasetId = new ConcurrentHashMap<>();
    private final Map<String, FeedMessage> alertsByDatasetId = new ConcurrentHashMap<>();
    private final GtfsTripsService gtfsTripsService;

    public SiriToGtfsRealtimeService(AlertFactory alertFactory,
                                     RedisService redisService,
                                     PrometheusMetricsService prometheusMetricsService,
                                     IdMapper idMapper,
                                     @Value("${kishar.settings.vm.close.to.stop.percentage}") int closeToNextStopPercentage,
                                     @Value("${kishar.settings.vm.close.to.stop.distance}") int closeToNextStopDistance, GtfsTripsService gtfsTripsService) {
        this.alertFactory = alertFactory;
        this.redisService = redisService;
        this.idMapper = idMapper;
        this.gtfsRtMapper = new GtfsRtMapper(closeToNextStopPercentage, closeToNextStopDistance);
        this.prometheusMetricsService = prometheusMetricsService;
        this.gtfsTripsService = gtfsTripsService;
    }

    public void clearGtfsRtCache() {
        LOG.warn("Clear all GTFS-RT data");
        tripUpdatesByDatasetId.clear();
        vehiclePositionsByDatasetId.clear();
        alertsByDatasetId.clear();
    }

    public void reset() {
        LOG.warn("Resetting ALL data");
        redisService.resetAllData();
    }

    public String getStatus() {
        ArrayList<String> status = new ArrayList<>();
        status.add("tripUpdates: " + tripUpdatesByDatasetId.values().stream().mapToInt(FeedMessage::getEntityCount).sum());
        status.add("vehiclePositions: " + vehiclePositionsByDatasetId.values().stream().mapToInt(FeedMessage::getEntityCount).sum());
        status.add("alerts: " + alertsByDatasetId.values().stream().mapToInt(FeedMessage::getEntityCount).sum());
        return status.toString();
    }

    public Object getTripUpdates(String contentType, String datasetId, boolean useOriginalId) {
        if (prometheusMetricsService != null) {
            prometheusMetricsService.registerIncomingRequest("SIRI_ET", 1);
        }
        FeedMessage feedMessage = tripUpdatesByDatasetId.get(datasetId.toUpperCase());
        int nbOfMessageBeforeFiltering = 0;
        if (feedMessage == null) {
            feedMessage = createFeedMessageBuilder().build();
        }else{
            nbOfMessageBeforeFiltering = feedMessage.getEntityCount();
        }
        feedMessage = filterDecreasingStopUpdates(feedMessage);
        LOG.info("TripUpdate - before filtering:{} - after filtering : {}", nbOfMessageBeforeFiltering, feedMessage.getEntityCount());
        feedMessage = idMapper.applyIdProcessingParameters(feedMessage, datasetId, useOriginalId);
        return encodeFeedMessage(feedMessage, contentType);
    }

    public FeedMessage filterDecreasingStopUpdates(FeedMessage feedMessage) {
        if (feedMessage == null || CollectionUtils.isEmpty(feedMessage.getEntityList())) {
            return feedMessage;
        }
        FeedMessage.Builder processedMessage = FeedMessage.newBuilder();
        processedMessage.setHeader(feedMessage.getHeader());


        List<FeedEntity> processedEntities = new ArrayList<>();
        for (FeedEntity feedEntity : feedMessage.getEntityList()) {
            if (feedEntity.getTripUpdate() == null) {
                processedEntities.add(feedEntity);
            }

            FeedEntity.Builder processedEntityBuilder = FeedEntity.newBuilder();
            processedEntityBuilder.setId(feedEntity.getId());
            processedEntityBuilder.setTripUpdate(filterDecreasingUpdates(feedEntity.getTripUpdate()));
            processedEntities.add(processedEntityBuilder.build());
        }
        processedMessage.addAllEntity(processedEntities);

        return processedMessage.build();
    }

    private TripUpdate filterDecreasingUpdates(TripUpdate tripUpdate) {
        TripUpdate.Builder processedTripUpdate = TripUpdate.newBuilder();
        if (tripUpdate.hasTrip()) {
            processedTripUpdate.setTrip(tripUpdate.getTrip());
        }

        if (tripUpdate.hasVehicle()) {
            processedTripUpdate.setVehicle(tripUpdate.getVehicle());
        }

        List<TripUpdate.StopTimeUpdate> originalUpdates = new ArrayList<>(tripUpdate.getStopTimeUpdateList());
        originalUpdates.sort(new StopTimeUpdateComparator());
        long lastDepartureTime = 0;
        long lastArrivalTime = 0;

        for (TripUpdate.StopTimeUpdate originalStu : originalUpdates) {

            if (isDepartureAscending(originalStu, lastDepartureTime) && isArrivalAscending(originalStu, lastArrivalTime)) {
                processedTripUpdate.addStopTimeUpdate(originalStu);
                if (originalStu.hasDeparture() && originalStu.getDeparture().hasTime()) {
                    lastDepartureTime = originalStu.getDeparture().getTime();
                }

                if (originalStu.hasArrival() && originalStu.getArrival().hasTime()) {
                    lastArrivalTime = originalStu.getArrival().getTime();
                }
            }
        }
        processedTripUpdate.setTimestamp(tripUpdate.getTimestamp());
        return processedTripUpdate.build();
    }

    private boolean isArrivalAscending(TripUpdate.StopTimeUpdate originalStu, long lastArrivalTime) {
        return !originalStu.hasArrival() || !originalStu.getArrival().hasTime() || originalStu.getArrival().getTime() > lastArrivalTime;
    }

    private boolean isDepartureAscending(TripUpdate.StopTimeUpdate originalStu, long lastDepartureTime) {
        return !originalStu.hasDeparture() || !originalStu.getDeparture().hasTime() || originalStu.getDeparture().getTime() > lastDepartureTime;
    }

    public Object getVehiclePositions(String contentType, String datasetId, boolean useOriginalId) {
        if (prometheusMetricsService != null) {
            prometheusMetricsService.registerIncomingRequest("SIRI_VM", 1);
        }
        FeedMessage feedMessage = null;
        if (datasetId != null) {
            feedMessage = vehiclePositionsByDatasetId.get(datasetId.toUpperCase());
        }

        if (feedMessage == null) {
            feedMessage = createFeedMessageBuilder().build();
        }
        feedMessage = idMapper.applyIdProcessingParameters(feedMessage, datasetId, useOriginalId);
        return encodeFeedMessage(feedMessage, contentType);
    }

    public Object getAlerts(String contentType, String datasetId, boolean useOriginalId) {
        if (prometheusMetricsService != null) {
            prometheusMetricsService.registerIncomingRequest("SIRI_SX", 1);
        }

        FeedMessage feedMessage;
        feedMessage = alertsByDatasetId.get(datasetId.toUpperCase());
        if (feedMessage == null) {
            feedMessage = createFeedMessageBuilder().build();
        }
        feedMessage = idMapper.applyIdProcessingParameters(feedMessage, datasetId, useOriginalId);
        return encodeFeedMessage(feedMessage, contentType);
    }

    private Object encodeFeedMessage(FeedMessage feedMessage, String contentType) {

        if (contentType != null && contentType.equals(MEDIA_TYPE_APPLICATION_JSON)) {
            return feedMessage;
        }
        if (feedMessage != null) {
            return feedMessage.toByteArray();
        }
        return null;
    }

    private void checkPreconditions(VehicleActivityStructure vehicleActivity) {
        checkPreconditions(vehicleActivity, true);
    }

    private void checkPreconditions(VehicleActivityStructure vehicleActivity, boolean countMetric) {

        Preconditions.checkState(vehicleActivity.hasMonitoredVehicleJourney(), "MonitoredVehicleJourney");

        String datasource = vehicleActivity.getMonitoredVehicleJourney().getDataSource();
        Preconditions.checkNotNull(datasource, "datasource");

        if (countMetric && prometheusMetricsService != null) {
            prometheusMetricsService.registerIncomingEntity("SIRI_VM", 1, false);
        }


        Preconditions.checkState(vehicleActivity.getMonitoredVehicleJourney().hasFramedVehicleJourneyRef());
        checkPreconditions(vehicleActivity.getMonitoredVehicleJourney().getFramedVehicleJourneyRef());

    }

    private void checkPreconditions(EstimatedVehicleJourneyStructure estimatedVehicleJourney) {

        Preconditions.checkState(estimatedVehicleJourney.hasFramedVehicleJourneyRef());
        checkPreconditions(estimatedVehicleJourney.getFramedVehicleJourneyRef());

        String datasource = estimatedVehicleJourney.getDataSource();
        Preconditions.checkNotNull(datasource, "datasource");
        if (prometheusMetricsService != null) {
            prometheusMetricsService.registerIncomingEntity("SIRI_ET", 1, false);
        }
        Preconditions.checkNotNull(estimatedVehicleJourney.getEstimatedCalls(), "EstimatedCalls");
        Preconditions.checkState(estimatedVehicleJourney.getEstimatedCalls().getEstimatedCallCount() > 0, "EstimatedCalls not empty");
    }

    private void checkPreconditions(PtSituationElementStructure situation) {
        Preconditions.checkNotNull(situation.getSituationNumber());
        Preconditions.checkNotNull(situation.getSituationNumber().getValue());

        Preconditions.checkState(
                !situation.getProgress().equals(WorkflowStatusEnumeration.WORKFLOW_STATUS_ENUMERATION_CLOSED),
                "Ignore message with Progress=closed"
        );

        Preconditions.checkNotNull(situation.getParticipantRef(), "datasource");
        String datasource = situation.getParticipantRef().getValue();
        Preconditions.checkNotNull(datasource, "datasource");
        if (prometheusMetricsService != null) {
            prometheusMetricsService.registerIncomingEntity("SIRI_SX", 1, false);
        }
    }

    private void checkPreconditions(FramedVehicleJourneyRefStructure fvjRef) {
        Preconditions.checkState(fvjRef.hasDataFrameRef(), "DataFrameRef");
        Preconditions.checkNotNull(fvjRef.getDataFrameRef().getValue(), "DataFrameRef");
        Preconditions.checkNotNull(fvjRef.getDatedVehicleJourneyRef(), "DatedVehicleJourneyRef");
    }

    private void checkPostconditions(FeedEntityOrBuilder entity, String datasetId) {
        Preconditions.checkNotNull(entity, "entity must not be null");
        if (entity.hasAlert()) {
            for (var es : entity.getAlert().getInformedEntityList()) {
                if (es.hasTrip()) {
                    checkPostconditions(es.getTrip(), datasetId);
                }
            }
        } else if (entity.hasVehicle()) {
            if (entity.getVehicle().hasTrip()) {
                checkPostconditions(entity.getVehicle().getTrip(), datasetId);
            }
        } else if (entity.hasTripUpdate()) {
            checkPostconditions(entity.getTripUpdate().getTrip(), datasetId);
        }
    }

    private void checkPostconditions(TripDescriptor td, String datasetId) {
        Preconditions.checkNotNull(td, "TripDescriptor must not be null");
        if (!td.hasTripId()) {
            Preconditions.checkState(td.hasRouteId() && td.hasDirectionId() && td.hasStartDate() && td.hasStartTime(), "if the trip_id field can't be provided, then route_id, direction_id, start_date, and start_time must all be provided");
        } else {
            if (gtfsTripsService.isDatasetInCache(datasetId.toUpperCase())) {
                // this is mandatory to check if dataset is in cache because it will be in cache iff GTFS has been
                // imported to this dataset
                // without this check it would reject all GTFS-RT data on datasets where no GTFS import occurred
                Preconditions.checkState(gtfsTripsService.existsTripByDatasetIdAndTripId(datasetId.toUpperCase(), td.getTripId()),
                        "trip_id %s not found in dataset %s", td.getTripId(), datasetId.toUpperCase());
            }
        }
    }

    private TripAndVehicleKey getKey(VehicleActivityStructure vehicleActivity) {

        VehicleActivityStructure.MonitoredVehicleJourneyType mvj = vehicleActivity.getMonitoredVehicleJourney();

        return getTripAndVehicleKey(mvj.hasVehicleRef() ? mvj.getVehicleRef() : null, mvj.getFramedVehicleJourneyRef());
    }

    private TripAndVehicleKey getTripAndVehicleKey(VehicleRefStructure vehicleRef, FramedVehicleJourneyRefStructure fvjRef) {
        String vehicle = null;
        if (vehicleRef != null && vehicleRef.getValue() != null) {
            vehicle = vehicleRef.getValue();
        }

        return TripAndVehicleKey.fromTripIdServiceDateAndVehicleId(
                fvjRef.getDatedVehicleJourneyRef(), fvjRef.getDataFrameRef().getValue(), vehicle);
    }

    public void writeOutput() {
        long t1 = System.currentTimeMillis();
        writeTripUpdates();
        writeVehiclePositions();
        writeAlerts();

        if (prometheusMetricsService != null) {
            prometheusMetricsService.registerTotalGtfsRtEntities(
                    tripUpdatesByDatasetId.values().stream().mapToInt(FeedMessage::getEntityCount).sum(),
                    vehiclePositionsByDatasetId.values().stream().mapToInt(FeedMessage::getEntityCount).sum(),
                    alertsByDatasetId.values().stream().mapToInt(FeedMessage::getEntityCount).sum());
        }


        tripUpdatesByDatasetId.entrySet().forEach(entry -> prometheusMetricsService.registerTotalGtfsRtEntitiesByDataset(entry.getKey(),"TRIP_UPDATE", entry.getValue().getEntityCount()));
        vehiclePositionsByDatasetId.entrySet().forEach(entry -> prometheusMetricsService.registerTotalGtfsRtEntitiesByDataset(entry.getKey(),"VEHICLE_POSITION", entry.getValue().getEntityCount()));
        alertsByDatasetId.entrySet().forEach(entry -> prometheusMetricsService.registerTotalGtfsRtEntitiesByDataset(entry.getKey(),"ALERT", entry.getValue().getEntityCount()));



        LOG.info("Wrote output in {} ms: {} alerts, {} vehicle-positions, {} trip-updates",
                (System.currentTimeMillis() - t1),
                alertsByDatasetId.values().stream().mapToInt(FeedMessage::getEntityCount).sum(),
                vehiclePositionsByDatasetId.values().stream().mapToInt(FeedMessage::getEntityCount).sum(),
                tripUpdatesByDatasetId.values().stream().mapToInt(FeedMessage::getEntityCount).sum());
    }

    private void writeTripUpdates() {

        FeedMessage.Builder feedMessageBuilder = createFeedMessageBuilder();
        Map<String, FeedMessage.Builder> feedMessageBuilderMap = Maps.newHashMap();

        Map<String, byte[]> tripUpdateMap = redisService.readGtfsRtMap(RedisService.Type.TRIP_UPDATE);

        for (String keyBytes : tripUpdateMap.keySet()) {
            CompositeKey key = CompositeKey.create(keyBytes);

            FeedEntity entity = null;
            try {
                byte[] data = tripUpdateMap.get(keyBytes);
//                data = Arrays.copyOfRange(data, 16, data.length);
                entity = FeedEntity.parseFrom(data);
            } catch (InvalidProtocolBufferException e) {
                LOG.error("invalid feed entity from reddis with key: " + key, e);
                continue;
            }

            String datasource = key.getDatasource();
            FeedMessage.Builder feedMessageBuilderByDatasource = feedMessageBuilderMap.get(datasource);
            if (feedMessageBuilderByDatasource == null) {
                feedMessageBuilderByDatasource = createFeedMessageBuilder();
            }

            feedMessageBuilder.addEntity(entity);
            feedMessageBuilderByDatasource.addEntity(entity);
            feedMessageBuilderMap.put(datasource, feedMessageBuilderByDatasource);
        }
        this.tripUpdatesByDatasetId.clear();
        this.tripUpdatesByDatasetId.putAll(buildFeedMessageMap(feedMessageBuilderMap));
    }

    private Map<String, FeedMessage> buildFeedMessageMap(Map<String, FeedMessage.Builder> feedMessageBuilderMap) {
        Map<String, FeedMessage> feedMessageMap = Maps.newHashMap();
        for (String key : feedMessageBuilderMap.keySet()) {
            feedMessageMap.put(key, feedMessageBuilderMap.get(key).build());
        }
        return feedMessageMap;
    }

    private String getTripIdForEstimatedVehicleJourney(EstimatedVehicleJourneyStructure mvj) {
        StringBuilder b = new StringBuilder();
        FramedVehicleJourneyRefStructure fvjRef = mvj.getFramedVehicleJourneyRef();
        b.append((fvjRef.getDatedVehicleJourneyRef()));
        b.append('-');
        b.append(fvjRef.getDataFrameRef().getValue());
        if (mvj.hasVehicleRef() && mvj.getVehicleRef().getValue() != null) {
            b.append('-');
            b.append(mvj.getVehicleRef().getValue());
        }
        return b.toString();
    }

    private String getKeyFromStopVisit(MonitoredStopVisitStructure stopVisit) {
        StringBuilder b = new StringBuilder();
        MonitoredVehicleJourneyStructure mvj = stopVisit.getMonitoredVehicleJourney();
        if (mvj.getFramedVehicleJourneyRef() != null && org.apache.commons.lang3.StringUtils.isNotEmpty(mvj.getFramedVehicleJourneyRef().getDatedVehicleJourneyRef())) {
            FramedVehicleJourneyRefStructure fvjRef = mvj.getFramedVehicleJourneyRef();
            b.append((fvjRef.getDatedVehicleJourneyRef()));
            b.append('-');
            b.append(fvjRef.getDataFrameRef().getValue());
            if (mvj.hasVehicleRef() && mvj.getVehicleRef().getValue() != null) {
                b.append('-');
                b.append(mvj.getVehicleRef().getValue());
            }
        } else {
            if (mvj.getLineRef() != null) {
                b.append(mvj.getLineRef().getValue() + "-");
            }

            if (mvj.getDirectionNameList() != null && !mvj.getDirectionNameList().isEmpty()) {
                NaturalLanguageStringStructure direction = mvj.getDirectionNameList().get(0);
                b.append(direction.getValue() + "-");
            }

            if (mvj.getMonitoredCall() != null && mvj.getMonitoredCall().getAimedArrivalTime() != null) {
                b.append(mvj.getMonitoredCall().getAimedArrivalTime().getSeconds());
            }
        }

        return b.toString();
    }

    private void writeVehiclePositions() {

        FeedMessage.Builder feedMessageBuilder = createFeedMessageBuilder();
        Map<String, FeedMessage.Builder> feedMessageBuilderMap = Maps.newHashMap();

        Map<String, byte[]> vehiclePositionMap = redisService.readGtfsRtMap(RedisService.Type.VEHICLE_POSITION);

        for (String keyBytes : vehiclePositionMap.keySet()) {
            CompositeKey key = CompositeKey.create(keyBytes);
            FeedEntity entity = null;
            try {
                byte[] data = vehiclePositionMap.get(keyBytes);
//                data = Arrays.copyOfRange(data, 16, data.length);
                entity = FeedEntity.parseFrom(data);
            } catch (InvalidProtocolBufferException e) {
                LOG.error("invalid feed entity from redis with key: " + key, e);
                continue;
            }

            String datasource = key.getDatasource();

            FeedMessage.Builder feedMessageBuilderByDatasource = feedMessageBuilderMap.get(datasource);

            if (feedMessageBuilderByDatasource == null) {
                feedMessageBuilderByDatasource = createFeedMessageBuilder();
            }

            feedMessageBuilder.addEntity(entity);
            feedMessageBuilderByDatasource.addEntity(entity);
            feedMessageBuilderMap.put(datasource, feedMessageBuilderByDatasource);
        }

        this.vehiclePositionsByDatasetId.clear();
        this.vehiclePositionsByDatasetId.putAll(buildFeedMessageMap(feedMessageBuilderMap));
    }

    private String getVehicleIdForKey(TripAndVehicleKey key) {
        if (key.getVehicleId() != null) {
            return key.getVehicleId();
        }
        return key.getTripId() + "-"
                + key.getServiceDate();
    }

    private void writeAlerts() {
        FeedMessage.Builder feedMessageBuilder = createFeedMessageBuilder();
        Map<String, FeedMessage.Builder> feedMessageBuilderMap = Maps.newHashMap();

        Map<String, byte[]> alertMap = redisService.readGtfsRtMap(RedisService.Type.ALERT);

        for (String keyBytes : alertMap.keySet()) {
            CompositeKey key = CompositeKey.create(keyBytes);

            FeedEntity entity = null;
            try {
                byte[] data = alertMap.get(keyBytes);
//                data = Arrays.copyOfRange(data, 16, data.length);
                entity = FeedEntity.parseFrom(data);
            } catch (InvalidProtocolBufferException e) {
                LOG.error("invalid feed entity from reddis with key: " + key, e);
                continue;
            }

            String datasource = key.getDatasource();
            FeedMessage.Builder feedMessageBuilderByDatasource = feedMessageBuilderMap.get(datasource);
            if (feedMessageBuilderByDatasource == null) {
                feedMessageBuilderByDatasource = createFeedMessageBuilder();
            }

            feedMessageBuilder.addEntity(entity);
            feedMessageBuilderByDatasource.addEntity(entity);
            feedMessageBuilderMap.put(datasource, feedMessageBuilderByDatasource);
        }

        this.alertsByDatasetId.clear();
        this.alertsByDatasetId.putAll(buildFeedMessageMap(feedMessageBuilderMap));
    }

    public Map<String, GtfsRtData> convertSiriVmToGtfsRt(SiriType siri, String datasetId) {

        Map<String, GtfsRtData> result = Maps.newHashMap();

        if (siri == null) {
            return result;
        }
        ServiceDeliveryType serviceDelivery = siri.getServiceDelivery();
        if (serviceDelivery != null && serviceDelivery.getVehicleMonitoringDeliveryCount() > 0) {
            for (VehicleMonitoringDeliveryStructure deliveryStructure : serviceDelivery.getVehicleMonitoringDeliveryList()) {
                if (deliveryStructure != null && deliveryStructure.getVehicleActivityCount() > 0) {
                    for (VehicleActivityStructure activity : deliveryStructure.getVehicleActivityList()) {
                        try {
                            checkPreconditions(activity);
                            VehiclePosition.Builder builder = gtfsRtMapper.convertSiriToGtfsRt(datasetId, activity);
                            if (builder.getTimestamp() <= 0) {
                                builder.setTimestamp(System.currentTimeMillis());
                            }

                            FeedEntity.Builder entity = FeedEntity.newBuilder();
                            String key = getVehicleIdForKey(getKey(activity));
                            entity.setId(key);

                            entity.setVehicle(builder);
                            checkPostconditions(entity, datasetId);

                            Duration timeToLive;
                            if (activity.hasValidUntilTime()) {
                                timeToLive = Duration.newBuilder().setSeconds(Timestamps.between(SiriLibrary.getCurrentTime(), activity.getValidUntilTime()).getSeconds() + GRACE_PERIOD).build();
                            } else {
                                timeToLive = Duration.newBuilder().setSeconds(GRACE_PERIOD).build();
                            }

                            result.put(new CompositeKey(key, datasetId).asString(),
                                    new GtfsRtData(entity.build().toByteArray(), timeToLive));
                        } catch (Exception e) {
                            LOG.debug("Failed parsing vehicle activity", e);
                        }
                    }

                }
            }
        }
        return result;
    }

    public void registerGtfsRtVehiclePosition(Map<String, GtfsRtData> vehiclePositions) {
        redisService.writeGtfsRt(vehiclePositions, RedisService.Type.VEHICLE_POSITION);
    }

    public Map<String, GtfsRtData> convertSiriEtToGtfsRt(SiriType siri, String datasetId) {

        Map<String, GtfsRtData> result = Maps.newHashMap();

        if (siri == null) {
            return result;
        }
        ServiceDeliveryType serviceDelivery = siri.getServiceDelivery();
        if (serviceDelivery != null && serviceDelivery.getEstimatedTimetableDeliveryCount() > 0) {
            for (EstimatedTimetableDeliveryStructure estimatedTimetableDeliveryStructure : serviceDelivery.getEstimatedTimetableDeliveryList()) {
                if (estimatedTimetableDeliveryStructure != null && estimatedTimetableDeliveryStructure.getEstimatedJourneyVersionFrameCount() > 0) {
                    for (EstimatedVersionFrameStructure estimatedVersionFrameStructure : estimatedTimetableDeliveryStructure.getEstimatedJourneyVersionFrameList()) {
                        if (estimatedVersionFrameStructure != null && estimatedVersionFrameStructure.getEstimatedVehicleJourneyCount() > 0) {
                            for (EstimatedVehicleJourneyStructure estimatedVehicleJourney : estimatedVersionFrameStructure.getEstimatedVehicleJourneyList()) {
                                if (estimatedVehicleJourney != null) {
                                    try {
                                        checkPreconditions(estimatedVehicleJourney);
                                        TripUpdate.Builder builder = gtfsRtMapper.mapTripUpdateFromVehicleJourney(datasetId, estimatedVehicleJourney);

                                        FeedEntity.Builder entity = FeedEntity.newBuilder();
                                        String key = getTripIdForEstimatedVehicleJourney(estimatedVehicleJourney);
                                        entity.setId(key);

                                        entity.setTripUpdate(builder);
                                        checkPostconditions(entity, datasetId);

                                        Timestamp expirationTime = null;
                                        for (RecordedCallStructure recordedCall : estimatedVehicleJourney.getRecordedCalls().getRecordedCallList()) {
                                            if (recordedCall.hasExpectedArrivalTime()) {
                                                expirationTime = SiriLibrary.getLatestTimestamp(expirationTime, recordedCall.getExpectedArrivalTime());
                                            } else if (recordedCall.hasAimedArrivalTime()) {
                                                expirationTime = SiriLibrary.getLatestTimestamp(expirationTime, recordedCall.getAimedArrivalTime());
                                            } else if (recordedCall.hasExpectedDepartureTime()) {
                                                expirationTime = SiriLibrary.getLatestTimestamp(expirationTime, recordedCall.getExpectedDepartureTime());
                                            } else if (recordedCall.hasAimedDepartureTime()) {
                                                expirationTime = SiriLibrary.getLatestTimestamp(expirationTime, recordedCall.getAimedDepartureTime());
                                            }
                                        }
                                        for (EstimatedCallStructure estimatedCall : estimatedVehicleJourney.getEstimatedCalls().getEstimatedCallList()) {
                                            if (estimatedCall.hasExpectedArrivalTime()) {
                                                expirationTime = SiriLibrary.getLatestTimestamp(expirationTime, estimatedCall.getExpectedArrivalTime());
                                            } else if (estimatedCall.hasAimedArrivalTime()) {
                                                expirationTime = SiriLibrary.getLatestTimestamp(expirationTime, estimatedCall.getAimedArrivalTime());
                                            } else if (estimatedCall.hasExpectedDepartureTime()) {
                                                expirationTime = SiriLibrary.getLatestTimestamp(expirationTime, estimatedCall.getExpectedDepartureTime());
                                            } else if (estimatedCall.hasAimedDepartureTime()) {
                                                expirationTime = SiriLibrary.getLatestTimestamp(expirationTime, estimatedCall.getAimedDepartureTime());
                                            }
                                        }
                                        Duration timeToLive;
                                        if (expirationTime == null) {
                                            timeToLive = Duration.newBuilder().setSeconds(GRACE_PERIOD).build();
                                        } else {
                                            timeToLive = Timestamps.between(SiriLibrary.getCurrentTime(), expirationTime);
                                        }

                                        result.put(new CompositeKey(key, datasetId).asString(), new GtfsRtData(entity.build().toByteArray(), timeToLive));
                                    } catch (Exception e) {
                                        LOG.debug("Failed parsing trip updates", e);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return result;
    }

    public void registerGtfsRtTripUpdates(Map<String, GtfsRtData> tripUpdates) {
        redisService.writeGtfsRt(tripUpdates, RedisService.Type.TRIP_UPDATE);
    }

    public Map<String, GtfsRtData> convertSiriSmToGtfsRt(SiriType siri, String datasetId) {

        Map<String, GtfsRtData> result = Maps.newHashMap();

        if (siri == null) {
            return result;
        }

        ServiceDeliveryType serviceDelivery = siri.getServiceDelivery();
        if (serviceDelivery != null && serviceDelivery.getStopMonitoringDeliveryCount() > 0) {


            for (StopMonitoringDeliveryStructure stopMonitoringDeliveryStructure : serviceDelivery.getStopMonitoringDeliveryList()) {
                if (stopMonitoringDeliveryStructure != null && stopMonitoringDeliveryStructure.getMonitoredStopVisitCount() > 0) {
                    for (MonitoredStopVisitStructure monitoredStopVisitStructure : stopMonitoringDeliveryStructure.getMonitoredStopVisitList()) {

                        try {
                            checkPreconditions(monitoredStopVisitStructure);
                            TripUpdate.Builder builder = gtfsRtMapper.mapTripUpdateFromStopVisit(datasetId, monitoredStopVisitStructure);

                            FeedEntity.Builder entity = FeedEntity.newBuilder();
                            String key = getKeyFromStopVisit(monitoredStopVisitStructure);
                            entity.setId(key);

                            entity.setTripUpdate(builder);
                            checkPostconditions(entity, datasetId);

                            Timestamp expirationTime = getExpirationDate(monitoredStopVisitStructure);
                            Duration timeToLive;

                            if (expirationTime == null) {
                                timeToLive = Duration.newBuilder().setSeconds(GRACE_PERIOD).build();
                            } else {
                                timeToLive = Timestamps.between(SiriLibrary.getCurrentTime(), expirationTime);
                            }

                            result.put(new CompositeKey(key, datasetId).asString(),
                                    new GtfsRtData(entity.build().toByteArray(), timeToLive));
                        } catch (Exception e) {
                            LOG.debug("Failed parsing vehicle activity", e);
                        }
                    }

                }
            }
        }

        return result;
    }


    private Timestamp getExpirationDate(MonitoredStopVisitStructure monitoredStopVisitStructure) {


        MonitoredCallStructure monitoredCall = monitoredStopVisitStructure.getMonitoredVehicleJourney().getMonitoredCall();

        if (monitoredCall.hasExpectedArrivalTime()) {
            return monitoredCall.getExpectedArrivalTime();
        } else if (monitoredCall.hasAimedArrivalTime()) {
            return monitoredCall.getAimedArrivalTime();
        } else if (monitoredCall.hasExpectedDepartureTime()) {
            return monitoredCall.getExpectedDepartureTime();
        } else if (monitoredCall.hasAimedDepartureTime()) {
            return monitoredCall.getAimedDepartureTime();
        }
        return null;
    }

    private void checkPreconditions(MonitoredStopVisitStructure monitoredStopVisitStructure) {
        Preconditions.checkState(monitoredStopVisitStructure.hasMonitoringRef(), "MonitoredRef");

    }

    public Map<String, GtfsRtData> convertSiriSxToGtfsRt(SiriType siri, String datasetId) {

        Map<String, GtfsRtData> result = Maps.newHashMap();

        if (siri == null) {
            return result;
        }
        ServiceDeliveryType serviceDelivery = siri.getServiceDelivery();
        if (serviceDelivery != null && serviceDelivery.getSituationExchangeDeliveryCount() > 0) {
            for (SituationExchangeDeliveryStructure situationExchangeDeliveryStructure : serviceDelivery.getSituationExchangeDeliveryList()) {
                if (situationExchangeDeliveryStructure != null && situationExchangeDeliveryStructure.getSituations() != null && situationExchangeDeliveryStructure.getSituations().getPtSituationElementCount() > 0) {
                    for (PtSituationElementStructure ptSituationElement : situationExchangeDeliveryStructure.getSituations().getPtSituationElementList()) {
                        if (ptSituationElement != null) {
                            try {
                                checkPreconditions(ptSituationElement);
                                Alert.Builder alertFromSituation = alertFactory.createAlertFromSituation(ptSituationElement);

                                FeedEntity.Builder entity = FeedEntity.newBuilder();
                                String key = ptSituationElement.getSituationNumber().getValue();
                                entity.setId(key);

                                entity.setAlert(alertFromSituation);
                                checkPostconditions(entity, datasetId);

                                Timestamp endTime = null;
                                for (HalfOpenTimestampOutputRangeStructure range : ptSituationElement.getValidityPeriodList()) {
                                    if (!range.hasEndTime()) {
                                        endTime = Timestamp.newBuilder().setSeconds(MAX_END_DATE).build();
                                        break;
                                    }
                                    Timestamp rangeEndTimestamp = range.getEndTime();
                                    endTime = SiriLibrary.getLatestTimestamp(endTime, rangeEndTimestamp);
                                }
                                Duration timeToLive;
                                if (endTime == null) {
                                    timeToLive = Duration.newBuilder().setSeconds(GRACE_PERIOD).build();
                                } else {
                                    timeToLive = Timestamps.between(SiriLibrary.getCurrentTime(), endTime);
                                }

                                result.put(new CompositeKey(key, datasetId).asString(), new GtfsRtData(entity.build().toByteArray(), timeToLive));
                            } catch (Exception e) {
                                LOG.debug("Failed parsing alerts", e);
                            }
                        }
                    }
                }
            }

        }
        return result;
    }

    public void registerGtfsRtAlerts(Map<String, GtfsRtData> alerts) {
        redisService.writeGtfsRt(alerts, RedisService.Type.ALERT);
    }

    public void clearCacheByDatasetId(String datasetId) {
        LOG.info("Clear cache for datasetId {}", datasetId);
        redisService.clearByDatasetId(datasetId);
        writeAlerts();
        writeTripUpdates();
        writeVehiclePositions();
    }

}
