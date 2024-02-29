package org.entur.kishar.gtfsrt;

import com.google.common.collect.Maps;
import com.google.transit.realtime.GtfsRealtime;
import org.entur.kishar.gtfsrt.domain.GtfsRtData;
import org.junit.Test;
import uk.org.siri.www.siri.ServiceDeliveryType;
import uk.org.siri.www.siri.SiriType;
import uk.org.siri.www.siri.SituationExchangeDeliveryStructure;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.*;
import static org.entur.kishar.gtfsrt.Helper.createPtSituationElement;
import static org.entur.kishar.gtfsrt.TestAlertFactory.assertAlert;
import static org.mockito.Mockito.when;

public class TestSiriSXToGtfsRealtimeService extends SiriToGtfsRealtimeServiceTest {

    @Test
    public void testSituationToAlert() throws IOException {
        SiriType siri = createSiriSx("TEST");

        Map<String, byte[]> redisMap = getRedisMap(rtService, siri);

        when(redisService.readGtfsRtMap(RedisService.Type.ALERT)).thenReturn(redisMap);
        rtService.writeOutput();
        Object alerts = rtService.getAlerts("application/json", "TEST", false);
        assertNotNull(alerts);
        assertTrue(alerts instanceof GtfsRealtime.FeedMessage);

        GtfsRealtime.FeedMessage feedMessage = (GtfsRealtime.FeedMessage) alerts;
        List<GtfsRealtime.FeedEntity> entityList = feedMessage.getEntityList();
        assertFalse(entityList.isEmpty());

        GtfsRealtime.FeedMessage byteArrayFeedMessage = GtfsRealtime.FeedMessage.parseFrom((byte[]) rtService.getAlerts(null, "TEST", true));
        assertEquals(feedMessage, byteArrayFeedMessage);

        GtfsRealtime.FeedEntity entity = feedMessage.getEntity(0);
        assertNotNull(entity);
        GtfsRealtime.Alert alert = entity.getAlert();
        assertNotNull(alert);

        assertAlert(alert);
    }

    private Map<String, byte[]> getRedisMap(SiriToGtfsRealtimeService rtService, SiriType siri) {
        Map<String, GtfsRtData> gtfsRt = rtService.convertSiriSxToGtfsRt(siri, "TEST");
        Map<String, byte[]> redisMap = Maps.newHashMap();
        for (String key : gtfsRt.keySet()) {
            byte[] data = gtfsRt.get(key).getData();
            redisMap.put(key, data);
        }
        return redisMap;
    }

    @Test
    public void testSituationToAlertWithDatasetIdFiltering() throws IOException {

        String datasetId = "BNR";
        SiriType siri = createSiriSx(datasetId);

        Map<String, byte[]> redisMap = getRedisMap(rtService, siri);

        when(redisService.readGtfsRtMap(RedisService.Type.ALERT)).thenReturn(redisMap);
        rtService.writeOutput();
        Object alerts = rtService.getAlerts("application/json", null, false);
        assertNotNull(alerts);
        assertTrue(alerts instanceof GtfsRealtime.FeedMessage);
    }

    @Test
    public void testMappingOfSiriSx() {
        String datasetId = "RUT";

        SiriType siri = createSiriSx(datasetId);

        Map<String, GtfsRtData> result = rtService.convertSiriSxToGtfsRt(siri, "TEST");

        assertFalse(result.isEmpty());
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
