package org.entur.kishar.gtfsrt;

import com.google.common.collect.Maps;
import com.google.transit.realtime.GtfsRealtime;
import org.entur.kishar.gtfsrt.domain.GtfsRtData;
import org.junit.jupiter.api.Test;
import uk.org.siri.www.siri.ServiceDeliveryType;
import uk.org.siri.www.siri.SiriType;
import uk.org.siri.www.siri.SituationExchangeDeliveryStructure;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.entur.kishar.gtfsrt.Helper.createPtSituationElement;
import static org.entur.kishar.gtfsrt.Helper.createPtSituationElementWithoutEndDateValidityPeriod;
import static org.entur.kishar.gtfsrt.TestAlertFactory.assertAlert;
import static org.entur.kishar.utils.Constants.MAX_END_DATE;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

class TestMergedAlerts extends SiriToGtfsRealtimeServiceTest {

    @Test
    void testMergedDatasets() throws IOException {
        String datasetId1 = "DAT1";
        String datasetId2 = "DAT2";

        SiriType siri = createSiriSx(datasetId1);
        SiriType siri2 = createSiriSx(datasetId2);

        Map<String, byte[]> redisMap = getRedisMap(rtService, siri, datasetId1);
        redisMap = addToRedisMap(redisMap, rtService, siri2, datasetId2);

        when(redisService.readGtfsRtMap(RedisService.Type.ALERT)).thenReturn(redisMap);
        when(subscriptionConfig.getIdParametersForDataset(anyString())).thenReturn(new HashMap<>());
        rtService.writeOutput();


        // Check retrieve for dataset 1
        Object alerts = rtService.getAlerts("application/json", datasetId1, true);
        assertNotNull(alerts);
        assertInstanceOf(GtfsRealtime.FeedMessage.class, alerts);

        GtfsRealtime.FeedMessage feedMessage = (GtfsRealtime.FeedMessage) alerts;
        List<GtfsRealtime.FeedEntity> entityList = feedMessage.getEntityList();
        assertFalse(entityList.isEmpty());

        GtfsRealtime.FeedMessage byteArrayFeedMessage = GtfsRealtime.FeedMessage.parseFrom((byte[]) rtService.getAlerts(null, datasetId1, true));
        assertEquals(feedMessage, byteArrayFeedMessage);

        GtfsRealtime.FeedEntity entity = feedMessage.getEntity(0);
        assertNotNull(entity);
        GtfsRealtime.Alert alert = entity.getAlert();
        assertNotNull(alert);
        assertAlert(alert);

        // Check retrieve for dataset 2
        Object alerts2 = rtService.getAlerts("application/json", datasetId2, true);
        assertNotNull(alerts2);
        assertInstanceOf(GtfsRealtime.FeedMessage.class, alerts2);
        GtfsRealtime.FeedMessage feedMessage2 = (GtfsRealtime.FeedMessage) alerts2;
        List<GtfsRealtime.FeedEntity> entityList2 = feedMessage2.getEntityList();
        assertFalse(entityList2.isEmpty());
        assertEquals(1, entityList2.size());
        GtfsRealtime.FeedEntity entity2 = feedMessage2.getEntity(0);
        assertNotNull(entity2);
        GtfsRealtime.Alert alert2 = entity2.getAlert();
        assertNotNull(alert2);
        assertAlert(alert2);

        // Check retrieve for cumulated datasets (all route)
        Object alerts3 = rtService.getAlertsForAllDatasets("application/json", true);
        assertNotNull(alerts3);
        assertInstanceOf(GtfsRealtime.FeedMessage.class, alerts3);
        GtfsRealtime.FeedMessage feedMessage3 = (GtfsRealtime.FeedMessage) alerts3;
        List<GtfsRealtime.FeedEntity> entityList3 = feedMessage3.getEntityList();
        assertFalse(entityList3.isEmpty());
        assertEquals(2, entityList3.size());
        GtfsRealtime.FeedEntity entity3 = feedMessage3.getEntity(0);
        assertNotNull(entity3);
        GtfsRealtime.Alert alert3 = entity3.getAlert();
        assertNotNull(alert3);
        assertAlert(alert3);


        // Check retrieve for cumulated datasets (comma list)
        Object alerts4 = rtService.getAlerts("application/json", datasetId1 + "," + datasetId2,true);
        assertNotNull(alerts4);
        assertInstanceOf(GtfsRealtime.FeedMessage.class, alerts4);
        GtfsRealtime.FeedMessage feedMessage4 = (GtfsRealtime.FeedMessage) alerts4;
        List<GtfsRealtime.FeedEntity> entityList4 = feedMessage4.getEntityList();
        assertFalse(entityList4.isEmpty());
        assertEquals(2, entityList4.size());
        GtfsRealtime.FeedEntity entity4 = feedMessage4.getEntity(0);
        assertNotNull(entity4);
        GtfsRealtime.Alert alert4 = entity4.getAlert();
        assertNotNull(alert4);
        assertAlert(alert4);
    }

    private Map<String, byte[]> getRedisMap(SiriToGtfsRealtimeService rtService, SiriType siri, String datasetId) {
        Map<String, GtfsRtData> gtfsRt = rtService.convertSiriSxToGtfsRt(siri, datasetId);
        Map<String, byte[]> redisMap = Maps.newHashMap();
        for (String key : gtfsRt.keySet()) {
            byte[] data = gtfsRt.get(key).getData();
            redisMap.put(key, data);
        }
        return redisMap;
    }

    private Map<String, byte[]> addToRedisMap(Map<String, byte[]> redisMap,  SiriToGtfsRealtimeService rtService, SiriType siri, String datasetId) {
        Map<String, GtfsRtData> gtfsRt = rtService.convertSiriSxToGtfsRt(siri, datasetId);
        for (String key : gtfsRt.keySet()) {
            byte[] data = gtfsRt.get(key).getData();
            redisMap.put(key, data);
        }
        return redisMap;
    }


    private SiriType createSiriSx(String datasetId) {
        SituationExchangeDeliveryStructure.SituationsType situations = SituationExchangeDeliveryStructure.SituationsType.newBuilder()
                .addPtSituationElement(createPtSituationElement(datasetId))
                .build();

        SituationExchangeDeliveryStructure sxDelivery = SituationExchangeDeliveryStructure.newBuilder()
                .setSituations(situations)
                .build();

        ServiceDeliveryType serviceDelivery = ServiceDeliveryType.newBuilder()
                .addSituationExchangeDelivery(sxDelivery)
                .build();

        return SiriType.newBuilder()
                .setServiceDelivery(serviceDelivery)
                .build();
    }

}
