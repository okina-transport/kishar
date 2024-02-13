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
import com.google.protobuf.Duration;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;

import com.google.common.collect.Maps;
import com.google.protobuf.util.Timestamps;
import com.google.transit.realtime.GtfsRealtime;
import com.google.transit.realtime.GtfsRealtime.*;
import org.entur.kishar.gtfsrt.domain.CompositeKey;
import org.entur.kishar.gtfsrt.domain.GtfsRtData;
import org.entur.kishar.gtfsrt.helpers.SiriLibrary;
import org.entur.kishar.gtfsrt.mappers.GtfsRtMapper;
import org.entur.kishar.metrics.PrometheusMetricsService;
import org.entur.kishar.utils.IdProcessingParameters;
import org.entur.kishar.utils.ObjectType;
import org.entur.kishar.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;
import uk.org.siri.www.siri.*;

import java.util.*;
import java.util.stream.Collectors;

import static org.entur.kishar.gtfsrt.helpers.GtfsRealtimeLibrary.createFeedMessageBuilder;

@Service
@Configuration
public class SiriToGtfsRealtimeService {
    private static Logger LOG = LoggerFactory.getLogger(SiriToGtfsRealtimeService.class);

    public static final String MONITORING_ERROR_NO_CURRENT_INFORMATION = "NO_CURRENT_INFORMATION";

    public static final String MONITORING_ERROR_NOMINALLY_LOCATED = "NOMINALLY_LOCATED";

    private static final String MEDIA_TYPE_APPLICATION_JSON = "application/json";

    private AlertFactory alertFactory;

    private boolean newData = false;

    private List<String> datasourceETWhitelist;

    private List<String> datasourceVMWhitelist;

    private List<String> datasourceSXWhitelist;

    @Autowired
    private PrometheusMetricsService prometheusMetricsService;

    private RedisService redisService;

    /**
     * Time, in seconds, after which a vehicle update is considered stale
     */
    private static final int gracePeriod = 5 * 60;
    private Map<String, FeedMessage> tripUpdatesByDatasetId = Maps.newHashMap();
    private Map<String, FeedMessage> vehiclePositionsByDatasetId = Maps.newHashMap();
    private Map<String, FeedMessage> alertsByDatasetId = Maps.newHashMap();

    private GtfsRtMapper gtfsMapper;

    public SiriToGtfsRealtimeService(@Autowired AlertFactory alertFactory,
                                     @Autowired RedisService redisService,
                                     @Value("${kishar.datasource.et.whitelist}") List<String> datasourceETWhitelist,
                                     @Value("${kishar.datasource.vm.whitelist}") List<String> datasourceVMWhitelist,
                                     @Value("${kishar.datasource.sx.whitelist}") List<String> datasourceSXWhitelist,
                                     @Value("${kishar.settings.vm.close.to.stop.percentage}") int closeToNextStopPercentage,
                                     @Value("${kishar.settings.vm.close.to.stop.distance}") int closeToNextStopDistance) {
        this.datasourceETWhitelist = datasourceETWhitelist;
        this.datasourceVMWhitelist = datasourceVMWhitelist;
        this.datasourceSXWhitelist = datasourceSXWhitelist;
        this.alertFactory = alertFactory;
        this.redisService = redisService;
        this.gtfsMapper = new GtfsRtMapper(closeToNextStopPercentage, closeToNextStopDistance);
    }

    public void reset() {
        LOG.warn("Resetting ALL data");
        redisService.resetAllData();
    }

    public String getStatus() {
        ArrayList<String> status = new ArrayList<>();
        status.add("tripUpdates: " + (int) tripUpdatesByDatasetId.values().stream()
                .map(FeedMessage::getEntityList).count());
        status.add("vehiclePositions: " +(int) vehiclePositionsByDatasetId.values().stream()
                .map(FeedMessage::getEntityList).count());
        status.add("alerts: " + (int) alertsByDatasetId.values().stream()
                .map(FeedMessage::getEntityList).count());
        return status.toString();
    }

    public Object getTripUpdates(String contentType, String datasetId, boolean useOriginalId) {
        if (prometheusMetricsService != null) {
            prometheusMetricsService.registerIncomingRequest("SIRI_ET", 1);
        }
        FeedMessage feedMessage;
        if (datasetId != null && !datasetId.isEmpty()) {
            feedMessage = tripUpdatesByDatasetId.get(datasetId.toUpperCase());
            if (feedMessage == null) {
                feedMessage = createFeedMessageBuilder().build();
            }
        } else {
            feedMessage = getTripUpdates();
        }
        //todo mapping tripUpdate et alerte id à revoir vehicle ok
        if(!useOriginalId){
            feedMessage = feedMessage.toBuilder().clearEntity().addAllEntity(feedMessage.getEntityList().stream()
                            .map(entity -> {
                                FeedEntity.Builder entityBuilder = entity.toBuilder();
                                mapMobiitiStopId(datasetId, entityBuilder); // Passer le Builder à la méthode
                                return entityBuilder.build();
                            })
                            .collect(Collectors.toList()))
                    .build();
        }
        return encodeFeedMessage(feedMessage, contentType);
    }

    public Object getVehiclePositions(String contentType, String datasetId) {
        if (prometheusMetricsService != null) {
            prometheusMetricsService.registerIncomingRequest("SIRI_VM", 1);
        }
        FeedMessage feedMessage;
        if (datasetId != null && !datasetId.isEmpty()) {
            feedMessage = vehiclePositionsByDatasetId.get(datasetId.toUpperCase());
            if (feedMessage == null) {
                feedMessage = createFeedMessageBuilder().build();
            }
        } else {
            feedMessage = getVehiclePositions();
        }
        return encodeFeedMessage(feedMessage, contentType);
    }

    public Object getAlerts(String contentType, String datasetId) {
        if (prometheusMetricsService != null) {
            prometheusMetricsService.registerIncomingRequest("SIRI_SX", 1);
        }
        FeedMessage feedMessage =  FeedMessage.newBuilder().build();
        if (datasetId != null && !datasetId.isEmpty()) {
            feedMessage = alertsByDatasetId.get(datasetId.toUpperCase());
            if (feedMessage == null) {
                feedMessage = createFeedMessageBuilder().build();
            }
        }else {
            feedMessage = getAlerts();
        }
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
        newData = false;
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

        setTripUpdates(feedMessageBuilder.build(), buildFeedMessageMap(feedMessageBuilderMap));
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

    private String getTripIdForMonitoredStopVisit(MonitoredStopVisitStructure stopVisit) {
        StringBuilder b = new StringBuilder();
        MonitoredVehicleJourneyStructure mvj = stopVisit.getMonitoredVehicleJourney();
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

        setVehiclePositions(feedMessageBuilder.build(), buildFeedMessageMap(feedMessageBuilderMap));
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

        setAlerts(feedMessageBuilder.build(), buildFeedMessageMap(feedMessageBuilderMap));
    }

    public FeedMessage getTripUpdates() {
        GtfsRealtime.FeedMessage.Builder mergedMessageBuilder = GtfsRealtime.FeedMessage.newBuilder();
        return tripUpdatesByDatasetId.values().stream().map(FeedMessage::toBuilder)
                .reduce(mergedMessageBuilder, (mergedBuilder, nextBuilder) -> {
                    mergedBuilder.mergeFrom(nextBuilder.build());
                    return mergedBuilder;
                })
                .build();
    }

    //todo arg 1 useless
    public void setTripUpdates(FeedMessage tripUpdates, Map<String, FeedMessage> tripUpdatesByDatasource) {
        this.tripUpdatesByDatasetId = tripUpdatesByDatasource;
    }

    public FeedMessage getVehiclePositions() {
        GtfsRealtime.FeedMessage.Builder mergedMessageBuilder = GtfsRealtime.FeedMessage.newBuilder();
        return vehiclePositionsByDatasetId.values().stream()
                .map(FeedMessage::toBuilder)
                .reduce(mergedMessageBuilder, (mergedBuilder, nextBuilder) -> {
                    mergedBuilder.mergeFrom(nextBuilder.build());
                    return mergedBuilder;
                })
                .build();
    }

    //todo arg 1 useless
    public void setVehiclePositions(FeedMessage vehiclePositions, Map<String, FeedMessage> vehiclePositionsByDatasource) {
        this.vehiclePositionsByDatasetId = vehiclePositionsByDatasource;
    }

    public FeedMessage getAlerts() {
        GtfsRealtime.FeedMessage.Builder mergedMessageBuilder = GtfsRealtime.FeedMessage.newBuilder();
        return alertsByDatasetId.values().stream()
                .map(FeedMessage::toBuilder)
                .reduce(mergedMessageBuilder, (mergedBuilder, nextBuilder) -> {
                    mergedBuilder.mergeFrom(nextBuilder.build());
                    return mergedBuilder;
                })
                .build();
    }

    //todo arg 1 useless
    public void setAlerts(FeedMessage alerts, Map<String, FeedMessage> alertsByDatasource) {
        this.alertsByDatasetId = alertsByDatasource;
    }

    public Map<String, GtfsRtData> convertSiriVmToGtfsRt(SiriType siri, String codespaceId) {

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
                            VehiclePosition.Builder builder = gtfsMapper.convertSiriToGtfsRt(activity);
                            if (builder.getTimestamp() <= 0) {
                                builder.setTimestamp(System.currentTimeMillis());
                            }

                            FeedEntity.Builder entity = FeedEntity.newBuilder();
                            String key = getVehicleIdForKey(getKey(activity));
                            entity.setId(key);

                            entity.setVehicle(builder);

                            Duration timeToLive;
                            if (activity.hasValidUntilTime()) {
                                timeToLive = Duration.newBuilder().setSeconds(Timestamps.between(SiriLibrary.getCurrentTime(), activity.getValidUntilTime()).getSeconds() + gracePeriod).build();
                            } else {
                                timeToLive = Duration.newBuilder().setSeconds(gracePeriod).build();
                            }

                            result.put(new CompositeKey(key, codespaceId).asString(),
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

    public Map<String, GtfsRtData> convertSiriEtToGtfsRt(SiriType siri, String codespaceId) {

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
                                        TripUpdate.Builder builder = gtfsMapper.mapTripUpdateFromVehicleJourney(codespaceId, estimatedVehicleJourney);

                                        FeedEntity.Builder entity = FeedEntity.newBuilder();
                                        String key = getTripIdForEstimatedVehicleJourney(estimatedVehicleJourney);
                                        entity.setId(key);

                                        entity.setTripUpdate(builder);

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
                                            timeToLive = Duration.newBuilder().setSeconds(gracePeriod).build();
                                        } else {
                                            timeToLive = Timestamps.between(SiriLibrary.getCurrentTime(), expirationTime);
                                        }

                                        result.put(new CompositeKey(key, codespaceId).asString(), new GtfsRtData(entity.build().toByteArray(), timeToLive));
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

    public Map<String, GtfsRtData> convertSiriSmToGtfsRt(SiriType siri, String codespaceId) {

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
                            TripUpdate.Builder builder = gtfsMapper.mapTripUpdateFromStopVisit(codespaceId, monitoredStopVisitStructure);

                            FeedEntity.Builder entity = FeedEntity.newBuilder();
                            String key = getTripIdForMonitoredStopVisit(monitoredStopVisitStructure);
                            entity.setId(key);

                            entity.setTripUpdate(builder);

                            Timestamp expirationTime = getExpirationDate(monitoredStopVisitStructure);
                            Duration timeToLive;

                            if (expirationTime == null) {
                                timeToLive = Duration.newBuilder().setSeconds(gracePeriod).build();
                            } else {
                                timeToLive = Timestamps.between(SiriLibrary.getCurrentTime(), expirationTime);
                            }

                            result.put(new CompositeKey(key, codespaceId).asString(),
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
        Preconditions.checkState(monitoredStopVisitStructure.getMonitoredVehicleJourney().hasFramedVehicleJourneyRef());
    }

    public Map<String, GtfsRtData> convertSiriSxToGtfsRt(SiriType siri, String codespaceId) {

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

                                Timestamp endTime = null;
                                for (HalfOpenTimestampOutputRangeStructure range : ptSituationElement.getValidityPeriodList()) {
                                    Timestamp rangeEndTimestamp = range.getEndTime();
                                    endTime = SiriLibrary.getLatestTimestamp(endTime, rangeEndTimestamp);
                                }
                                Duration timeToLive;
                                if (endTime == null) {
                                    timeToLive = Duration.newBuilder().setSeconds(gracePeriod).build();
                                } else {
                                    timeToLive = Timestamps.between(SiriLibrary.getCurrentTime(), endTime);
                                }

                                result.put(new CompositeKey(key, codespaceId).asString(), new GtfsRtData(entity.build().toByteArray(), timeToLive));
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

    private void mapMobiitiStopId(String provider, FeedEntity.Builder entityBuilder){
        GtfsRealtime.TripUpdate.Builder tripUpdateBuilder = entityBuilder.getTripUpdateBuilder();
        List<TripUpdate.StopTimeUpdate> updatedStopTimeUpdates = tripUpdateBuilder.getStopTimeUpdateBuilderList().stream()
                .map(stopTimeUpdateBuilder -> {
                    String updatedStopId = stopTimeUpdateBuilder.getStopId();
                    return stopTimeUpdateBuilder.setStopId(updatedStopId).build();
                })
                .collect(Collectors.toList());
        GtfsRealtime.TripUpdate updatedTripUpdate = tripUpdateBuilder.clearStopTimeUpdate().addAllStopTimeUpdate(updatedStopTimeUpdates).build();
        entityBuilder.setTripUpdate(updatedTripUpdate);
    }
}
