package org.entur.kishar.gtfsrt;

import com.google.common.collect.Maps;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import com.google.transit.realtime.GtfsRealtime;
import org.entur.kishar.gtfsrt.domain.GtfsRtData;
import org.entur.kishar.gtfsrt.helpers.SiriLibrary;
import org.entur.kishar.utils.ObjectType;
import org.junit.jupiter.api.Test;
import uk.org.siri.www.siri.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import static org.entur.kishar.gtfsrt.Helper.createFramedVehicleJourneyRefStructure;
import static org.entur.kishar.gtfsrt.Helper.createLineRef;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

class TestMergedTripUpdates extends SiriToGtfsRealtimeServiceTest {

    @Test
    void testGetTripUpdatesAllDatasetsWithEmptyCacheReturnsEmptyFeedMessage() throws IOException {
        // No data has been published to any dataset - cache is empty

        Object tripUpdates = rtService.getTripUpdatesAllDatasets("application/json", false);
        assertNotNull(tripUpdates);
        assertInstanceOf(GtfsRealtime.FeedMessage.class, tripUpdates);

        GtfsRealtime.FeedMessage feedMessage = (GtfsRealtime.FeedMessage) tripUpdates;
        assertTrue(feedMessage.getEntityList().isEmpty());

        // Assert binary format also returns a body instead of null
        Object binaryTripUpdates = rtService.getTripUpdatesAllDatasets(null, false);
        assertNotNull(binaryTripUpdates);
        assertInstanceOf(byte[].class, binaryTripUpdates);

        GtfsRealtime.FeedMessage byteArrayFeedMessage = GtfsRealtime.FeedMessage.parseFrom((byte[]) binaryTripUpdates);
        assertTrue(byteArrayFeedMessage.getEntityList().isEmpty());
    }

    @Test
    void testMergedTripUpdates() throws IOException {
        String lineRefValue = "TST:Line:1234";
        int stopCount = 5;
        int delayPerStop = 30;
        String datedVehicleJourneyRef = "TST:ServiceJourney:1234";
        String dat1 = "DAT1";
        String dat2 = "DAT2";

        SiriType siriDat1 = createSiriEtDelivery(lineRefValue, createEstimatedCalls(stopCount, delayPerStop), datedVehicleJourneyRef, dat1);
        SiriType siriDat2 = createSiriEtDelivery(lineRefValue, createEstimatedCalls(stopCount, delayPerStop), datedVehicleJourneyRef, dat2);

        Map<String, byte[]> redisMap = getRedisMap(rtService, siriDat1, dat1);
        redisMap = addToRedisMap(redisMap, rtService, siriDat2, dat2);

        when(redisService.readGtfsRtMap(RedisService.Type.TRIP_UPDATE)).thenReturn(redisMap);
        when(subscriptionConfig.getIdParametersForDataset(anyString())).thenReturn(new EnumMap<>(ObjectType.class));

        // GTFS-RT is produced asynchronously - should be empty at first

        Object tripUpdates = rtService.getTripUpdates("application/json", dat1, false);
        assertNotNull(tripUpdates);
        assertInstanceOf(GtfsRealtime.FeedMessage.class, tripUpdates);

        GtfsRealtime.FeedMessage feedMessage = (GtfsRealtime.FeedMessage) tripUpdates;
        List<GtfsRealtime.FeedEntity> entityList = feedMessage.getEntityList();
        assertTrue(entityList.isEmpty());

        // Assert json and binary format
        GtfsRealtime.FeedMessage byteArrayFeedMessage = GtfsRealtime.FeedMessage.parseFrom((byte[]) rtService.getTripUpdates(null, dat1, false));
        assertEquals(feedMessage, byteArrayFeedMessage);

        rtService.writeOutput();


        // CHECK if there is data for dataset DAT1
        tripUpdates = rtService.getTripUpdates("application/json", dat1, true);
        assertNotNull(tripUpdates);
        assertInstanceOf(GtfsRealtime.FeedMessage.class, tripUpdates);


        feedMessage = (GtfsRealtime.FeedMessage) tripUpdates;
        entityList = feedMessage.getEntityList();
        assertFalse(entityList.isEmpty());

        byteArrayFeedMessage = GtfsRealtime.FeedMessage.parseFrom((byte[]) rtService.getTripUpdates(null, dat1, true));
        assertEquals(feedMessage, byteArrayFeedMessage);

        // CHECK if there is data for dataset DAT2
        Object tripUpdatesDat2 = rtService.getTripUpdates("application/json", dat2, true);
        assertNotNull(tripUpdatesDat2);
        assertInstanceOf(GtfsRealtime.FeedMessage.class, tripUpdatesDat2);


        GtfsRealtime.FeedMessage feedMessage2 = (GtfsRealtime.FeedMessage) tripUpdatesDat2;
        List<GtfsRealtime.FeedEntity> entityList2 = feedMessage2.getEntityList();
        assertFalse(entityList2.isEmpty());


        // CHECK if there is data for dataset DAT1 and DAT2 cumulated (called by ALL route)

        Object tripUpdatesDatCumulated = rtService.getTripUpdatesAllDatasets("application/json", true);
        assertNotNull(tripUpdatesDatCumulated);
        assertInstanceOf(GtfsRealtime.FeedMessage.class, tripUpdatesDatCumulated);


        GtfsRealtime.FeedMessage feedMessageCumulated = (GtfsRealtime.FeedMessage) tripUpdatesDatCumulated;
        List<GtfsRealtime.FeedEntity> entityListCumulated = feedMessageCumulated.getEntityList();
        assertFalse(entityListCumulated.isEmpty());
        assertEquals(2, entityListCumulated.size());

        // CHECK if there is data for dataset DAT1 and DAT2 cumulated  (called by selection of 2 datasets)

        Object tripUpdatesDatCumulated2 = rtService.getTripUpdates("application/json", dat1 + "," + dat2, true);
        assertNotNull(tripUpdatesDatCumulated2);
        assertInstanceOf(GtfsRealtime.FeedMessage.class, tripUpdatesDatCumulated2);


        GtfsRealtime.FeedMessage feedMessageCumulated2 = (GtfsRealtime.FeedMessage) tripUpdatesDatCumulated2;
        List<GtfsRealtime.FeedEntity> entityListCumulated2 = feedMessageCumulated2.getEntityList();
        assertFalse(entityListCumulated2.isEmpty());
        assertEquals(2, entityListCumulated2.size());

    }

    private SiriType createSiriEtDelivery(String lineRefValue, List<? extends EstimatedCallStructure> calls, String datedVehicleJourneyRef, String datasetId) {

        EstimatedVehicleJourneyStructure.EstimatedCallsType estimatedCalls = EstimatedVehicleJourneyStructure.EstimatedCallsType.newBuilder()
                .addAllEstimatedCall(calls)
                .build();

        EstimatedVehicleJourneyStructure.Builder estimatedVehicleJourneyBuilder = EstimatedVehicleJourneyStructure.newBuilder()
                .setLineRef(createLineRef(lineRefValue))
                .setDataSource(datasetId)
                .setEstimatedCalls(estimatedCalls);

        if (datedVehicleJourneyRef != null) {
            estimatedVehicleJourneyBuilder.setFramedVehicleJourneyRef(createFramedVehicleJourneyRefStructure(datedVehicleJourneyRef));
        }

        EstimatedVehicleJourneyStructure estimatedVehicleJourney = estimatedVehicleJourneyBuilder.build();

        EstimatedVersionFrameStructure etVersionFrame = EstimatedVersionFrameStructure.newBuilder()
                .addEstimatedVehicleJourney(estimatedVehicleJourney)
                .build();

        EstimatedTimetableDeliveryStructure etDelivery = EstimatedTimetableDeliveryStructure.newBuilder()
                .addEstimatedJourneyVersionFrame(etVersionFrame)
                .build();

        ServiceDeliveryType serviceDelivery = ServiceDeliveryType.newBuilder()
                .addEstimatedTimetableDelivery(etDelivery)
                .build();

        return SiriType.newBuilder()
                .setServiceDelivery(serviceDelivery)
                .build();
    }

    private List<? extends EstimatedCallStructure> createEstimatedCalls(int stopCount, Integer addedDelayPerStop) {
        List<EstimatedCallStructure> calls = new ArrayList<>();
        Timestamp startTime = SiriLibrary.getCurrentTime();

        for (int i = 0; i < stopCount; i++) {
            StopPointRefStructure stopPointRef = StopPointRefStructure.newBuilder()
                    .setValue("TST:Quay:1234-" + i)
                    .build();


            EstimatedCallStructure.Builder call = EstimatedCallStructure.newBuilder()
                    .setStopPointRef(stopPointRef);

            startTime = Timestamps.add(startTime, Duration.newBuilder().setSeconds(60).build());
            if (i > 0) {
                call.setAimedArrivalTime(startTime);
                if (addedDelayPerStop != null) {
                    Timestamp expected = Timestamps.add(startTime, Duration.newBuilder().setSeconds(addedDelayPerStop).build());
                    call.setExpectedArrivalTime(expected);
                }
            }
            if (i < stopCount - 1) {
                call.setAimedDepartureTime(startTime);

                if (addedDelayPerStop != null) {
                    call.setExpectedDepartureTime(Timestamps.add(startTime, Duration.newBuilder().setSeconds(addedDelayPerStop).build()));
                }
            }
            calls.add(call.build());
        }
        return calls;
    }

    private Map<String, byte[]> getRedisMap(SiriToGtfsRealtimeService rtService, SiriType siri, String datasetId) {
        Map<String, GtfsRtData> gtfsRt = rtService.convertSiriEtToGtfsRt(siri, datasetId);
        Map<String, byte[]> redisMap = Maps.newHashMap();
        for (String key : gtfsRt.keySet()) {
            byte[] data = gtfsRt.get(key).getData();
            redisMap.put(key, data);
        }
        return redisMap;
    }

    private Map<String, byte[]> addToRedisMap(Map<String, byte[]> redisMap, SiriToGtfsRealtimeService rtService, SiriType siri, String datasetId) {
        Map<String, GtfsRtData> gtfsRt = rtService.convertSiriEtToGtfsRt(siri, datasetId);
        for (String key : gtfsRt.keySet()) {
            byte[] data = gtfsRt.get(key).getData();
            redisMap.put(key, data);
        }
        return redisMap;
    }

}
