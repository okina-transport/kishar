package org.entur.kishar.gtfsrt;

import com.google.common.collect.Maps;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import com.google.transit.realtime.GtfsRealtime;
import org.entur.kishar.gtfsrt.domain.GtfsRtData;
import org.entur.kishar.gtfsrt.helpers.SiriLibrary;
import org.junit.jupiter.api.Test;
import uk.org.siri.www.siri.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.entur.kishar.gtfsrt.Helper.createFramedVehicleJourneyRefStructure;
import static org.entur.kishar.gtfsrt.Helper.createLineRef;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

public class TestMergedVehiclePositions extends SiriToGtfsRealtimeServiceTest{

    @Test
    void testVMMergedDatasets() {

        String lineRefValue = "TST:Line:1234";
        double latitude = 10.56;
        double longitude = 59.63;
        String datedVehicleJourneyRef = "vj12345";
        String vehicleRefValue = "TST:Vehicle:1234";
        String datasetId1 = "DAT1";
        String datasetId2 = "DAT2";

        SiriType siri = createSiriVmDelivery(lineRefValue, latitude, longitude, datedVehicleJourneyRef, vehicleRefValue, datasetId1);
        SiriType siri2 = createSiriVmDelivery(lineRefValue, latitude, longitude, datedVehicleJourneyRef, vehicleRefValue, datasetId2);

        Map<String, byte[]> redisMap = getRedisMap(datasetId1, rtService, siri);
        redisMap = addToRedisMap(redisMap, siri2, rtService, datasetId2);

        when(redisService.readGtfsRtMap(RedisService.Type.VEHICLE_POSITION)).thenReturn(redisMap);
        when(redisService.handleFlexibleLine(any())).thenAnswer(invocation -> invocation.getArguments()[0]);
        rtService.writeOutput();

        // Check data is returned for dataset 1
        Object vehiclePositions = rtService.getVehiclePositions("application/json", datasetId1, true);
        assertNotNull(vehiclePositions);
        assertInstanceOf(GtfsRealtime.FeedMessage.class, vehiclePositions);

        GtfsRealtime.FeedMessage feedMessage = (GtfsRealtime.FeedMessage) vehiclePositions;
        List<GtfsRealtime.FeedEntity> entityList = feedMessage.getEntityList();

        assertEquals(1, ((GtfsRealtime.FeedMessage) vehiclePositions).getEntityCount());

        assertFalse(entityList.isEmpty());

        // Check data is returned for dataset 2
        Object vehiclePositions2 = rtService.getVehiclePositions("application/json", datasetId2, true);
        assertNotNull(vehiclePositions2);
        assertInstanceOf(GtfsRealtime.FeedMessage.class, vehiclePositions2);

        GtfsRealtime.FeedMessage feedMessage2 = (GtfsRealtime.FeedMessage) vehiclePositions2;
        List<GtfsRealtime.FeedEntity> entityList2 = feedMessage2.getEntityList();

        assertEquals(1, entityList2.size());


        // Check data is returned for dataset 1 and dataset 2 (all route)
        Object vehiclePositions3 = rtService.getVehiclePositionsForAllDatasets("application/json", true);
        assertNotNull(vehiclePositions3);
        assertInstanceOf(GtfsRealtime.FeedMessage.class, vehiclePositions3);
        GtfsRealtime.FeedMessage feedMessage3 = (GtfsRealtime.FeedMessage) vehiclePositions3;
        List<GtfsRealtime.FeedEntity> entityList3 = feedMessage3.getEntityList();
        assertEquals(2, entityList3.size());

        // Check data is returned for dataset 1 and dataset 2 (comma list)
        Object vehiclePositions4 = rtService.getVehiclePositions("application/json", datasetId1 + "," + datasetId2,true);
        assertNotNull(vehiclePositions4);
        assertInstanceOf(GtfsRealtime.FeedMessage.class, vehiclePositions4);
        GtfsRealtime.FeedMessage feedMessage4 = (GtfsRealtime.FeedMessage) vehiclePositions4;
        List<GtfsRealtime.FeedEntity> entityList4 = feedMessage4.getEntityList();
        assertEquals(2, entityList4.size());
    }


    private Map<String, byte[]> getRedisMap(String datasetId ,
            SiriToGtfsRealtimeService realtimeService, SiriType siri
    ) {
        Map<String, GtfsRtData> gtfsRt = realtimeService.convertSiriVmToGtfsRt(siri, datasetId);
        Map<String, byte[]> redisMap = Maps.newHashMap();
        for (String key : gtfsRt.keySet()) {
            byte[] data = gtfsRt.get(key).getData();
            redisMap.put(key, data);
        }
        return redisMap;
    }

    private Map<String, byte[]> addToRedisMap(Map<String, byte[]> redisMap, SiriType siri, SiriToGtfsRealtimeService realtimeService, String datasetId) {
        Map<String, GtfsRtData> gtfsRt = realtimeService.convertSiriVmToGtfsRt(siri, datasetId);
        for (String key : gtfsRt.keySet()) {
            byte[] data = gtfsRt.get(key).getData();
            redisMap.put(key, data);
        }
        return redisMap;
    }




    private SiriType createSiriVmDelivery(String lineRefValue, double latitude, double longitude, String datedVehicleJourneyRef, String vehicleRefValue, String datasetId) {

        VehicleActivityStructure.MonitoredVehicleJourneyType.Builder mvjBuilder = VehicleActivityStructure.MonitoredVehicleJourneyType.newBuilder()
                .setLineRef(createLineRef(lineRefValue))
                .setVehicleRef(createVehicleRef(vehicleRefValue))
                .setVehicleLocation(createLocation(longitude, latitude))
                .setDataSource(datasetId);

        if (datedVehicleJourneyRef != null) {
            mvjBuilder.setFramedVehicleJourneyRef(Helper.createFramedVehicleJourneyRefStructure(datedVehicleJourneyRef));
        }

        VehicleActivityStructure.MonitoredVehicleJourneyType mvj = mvjBuilder.build();

        VehicleActivityStructure activity = VehicleActivityStructure.newBuilder()
                .setMonitoredVehicleJourney(mvj)
                .setRecordedAtTime(SiriLibrary.getCurrentTime())
                .setValidUntilTime(Timestamps.add(SiriLibrary.getCurrentTime(), Duration.newBuilder().setSeconds(600).build()))
                .build();

        VehicleMonitoringDeliveryStructure vmDelivery = VehicleMonitoringDeliveryStructure.newBuilder()
                .addVehicleActivity(activity)
                .build();

        ServiceDeliveryType serviceDelivery = ServiceDeliveryType.newBuilder()
                .addVehicleMonitoringDelivery(vmDelivery)
                .build();

        return SiriType.newBuilder()
                .setServiceDelivery(serviceDelivery)
                .build();
    }

    private VehicleRefStructure createVehicleRef(String value) {
        return VehicleRefStructure.newBuilder()
                .setValue(value)
                .build();
    }

    private LocationStructure createLocation(double longitude, double latitude) {
        return LocationStructure.newBuilder()
                .setLongitude(longitude)
                .setLatitude(latitude)
                .build();
    }

}
