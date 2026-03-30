package org.entur.kishar.gtfsrt;


import com.google.transit.realtime.GtfsRealtime;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;


import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;

import static org.junit.Assert.assertEquals;

class TestSiriSMToGtfsRealtimeService extends SiriToGtfsRealtimeServiceTest{

    @Autowired
    RedisService tested;

    @Autowired
    SiriToGtfsRealtimeService siriToGtfsRealtimeService;


    @Test
    void test_filter_ascending_times() {
        GtfsRealtime.FeedMessage.Builder originalMessageBuilder = GtfsRealtime.FeedMessage.newBuilder();
        GtfsRealtime.FeedEntity.Builder entityBuilder = GtfsRealtime.FeedEntity.newBuilder();
        entityBuilder.setId("id1");
        GtfsRealtime.FeedHeader.Builder feedHeader = GtfsRealtime.FeedHeader.newBuilder();
        feedHeader.setTimestamp(1526);
        feedHeader.setGtfsRealtimeVersion("1.0");
        originalMessageBuilder.setHeader(feedHeader.build());
        GtfsRealtime.TripUpdate.Builder tripUpdateBuilder = GtfsRealtime.TripUpdate.newBuilder();

        GtfsRealtime.TripDescriptor.Builder tripDesc = GtfsRealtime.TripDescriptor.newBuilder();
        tripDesc.setTripId("trip1");
        tripUpdateBuilder.setTrip(tripDesc.build());





        // trip update2 should be filtered because update time 2 = update time 1 (10)
        GtfsRealtime.TripUpdate.StopTimeUpdate.Builder tripUpd2 = GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder();
        tripUpd2.setStopId("B");
        tripUpd2.setStopSequence(2);
        GtfsRealtime.TripUpdate.StopTimeEvent.Builder evt2 = GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder();
        evt2.setTime(10);
        tripUpd2.setDeparture(evt2.build());
        tripUpdateBuilder.addStopTimeUpdate(tripUpd2.build());


        // trip update3 should be kept because update time 3 > update time 1
        GtfsRealtime.TripUpdate.StopTimeUpdate.Builder tripUpd3 = GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder();
        tripUpd3.setStopId("C");
        tripUpd3.setStopSequence(3);
        GtfsRealtime.TripUpdate.StopTimeEvent.Builder evt3 = GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder();
        evt3.setTime(15);
        tripUpd3.setDeparture(evt3.build());
        tripUpdateBuilder.addStopTimeUpdate(tripUpd3.build());

        tripUpdateBuilder.setTimestamp(12345467);
        GtfsRealtime.TripUpdate.StopTimeUpdate.Builder tripUpd1 = GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder();
        tripUpd1.setStopId("A");
        tripUpd1.setStopSequence(1);
        GtfsRealtime.TripUpdate.StopTimeEvent.Builder evt1 = GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder();
        evt1.setTime(10);
        tripUpd1.setDeparture(evt1.build());
        tripUpdateBuilder.addStopTimeUpdate(tripUpd1.build());

        entityBuilder.setTripUpdate(tripUpdateBuilder.build());
        originalMessageBuilder.addEntity(entityBuilder.build());





        // launch filtering
        GtfsRealtime.FeedMessage result = siriToGtfsRealtimeService.filterDecreasingStopUpdates(originalMessageBuilder.build());
        Assertions.assertEquals(1, result.getEntityList().size());

        GtfsRealtime.TripUpdate tripUpdate = result.getEntityList().getFirst().getTripUpdate();
        Assertions.assertEquals(2,tripUpdate.getStopTimeUpdateList().size());
        List<String> expectedStopIds = List.of("A","C");
        for (GtfsRealtime.TripUpdate.StopTimeUpdate stopTimeUpdate : tripUpdate.getStopTimeUpdateList()) {
            // only stops A and C should be kept
            Assertions.assertTrue(expectedStopIds.contains(stopTimeUpdate.getStopId()));
        }
    }

    @Test
    void trip_updates_have_been_ordered_on_merge() {


        ZonedDateTime arrival = ZonedDateTime.of(2055, 8, 20, 12, 0, 0, 0, ZoneOffset.UTC);
        ZonedDateTime departure = ZonedDateTime.of(2055, 8, 20, 12, 0, 30, 0, ZoneOffset.UTC);
        GtfsRealtime.FeedEntity incomingEntity = generateEntity("STOP1", arrival, departure);

        ZonedDateTime arrival2 = ZonedDateTime.of(2055, 8, 20, 12, 10, 0, 0, ZoneOffset.UTC);
        ZonedDateTime departure2 = ZonedDateTime.of(2055, 8, 20, 12, 10, 30, 0, ZoneOffset.UTC);
        GtfsRealtime.FeedEntity existing = generateEntity("STOP2", arrival2, departure2);

        // Simulating that STOP2(12h10) was integrated before. It's the existing Entity.
        // STOP1(12h00) is the new incoming tripUpdate
        GtfsRealtime.TripUpdate mergedTrip = tested.buildMergedTripUpdate(incomingEntity, existing);


        // Expected : trip updates have been ordered and STOP1 is before STOP2

        Assertions.assertEquals("STOP1", mergedTrip.getStopTimeUpdate(0).getStopId());
        Assertions.assertEquals("STOP2", mergedTrip.getStopTimeUpdate(1).getStopId());
    }

    private GtfsRealtime.FeedEntity generateEntity(String stopId, ZonedDateTime arrival, ZonedDateTime departure) {
        GtfsRealtime.TripDescriptor tdesc1 = GtfsRealtime.TripDescriptor.newBuilder().setTripId("1").build();
        Instant departureInst = departure.toInstant();
        Instant arrivalInst = arrival.toInstant();

        GtfsRealtime.TripUpdate.StopTimeEvent steDeparture = GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder()
                .setTime(departureInst.getEpochSecond())
                .build();

        GtfsRealtime.TripUpdate.StopTimeEvent steArrival = GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder()
                .setTime(arrivalInst.getEpochSecond())
                .build();

        GtfsRealtime.TripUpdate.StopTimeUpdate stu1 = GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder()
                .setStopId(stopId)
                .setDeparture(steDeparture)
                .setArrival(steArrival)
                .build();


        GtfsRealtime.TripUpdate t1 = GtfsRealtime.TripUpdate.newBuilder()
                .setTrip(tdesc1)
                .addStopTimeUpdate(stu1)
                .build();

        return GtfsRealtime.FeedEntity.newBuilder()
                .setId("Id1")
                .setTripUpdate(t1)
                .build();
    }

    private GtfsRealtime.FeedEntity generateEntityWithDelay(String stopId, ZonedDateTime arrival, ZonedDateTime departure, int delay) {
        GtfsRealtime.TripDescriptor tdesc1 = GtfsRealtime.TripDescriptor.newBuilder().setTripId("1").build();
        Instant departureInst = departure.toInstant();
        Instant arrivalInst = arrival.toInstant();

        GtfsRealtime.TripUpdate.StopTimeEvent steDeparture = GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder()
                .setDelay(delay)
                .setTime(departureInst.getEpochSecond())
                .build();

        GtfsRealtime.TripUpdate.StopTimeEvent steArrival = GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder()
                .setDelay(delay)
                .setTime(arrivalInst.getEpochSecond())
                .build();

        GtfsRealtime.TripUpdate.StopTimeUpdate stu1 = GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder()
                .setStopId(stopId)
                .setDeparture(steDeparture)
                .setArrival(steArrival)
                .build();


        GtfsRealtime.TripUpdate t1 = GtfsRealtime.TripUpdate.newBuilder()
                .setTrip(tdesc1)
                .addStopTimeUpdate(stu1)
                .build();

        return GtfsRealtime.FeedEntity.newBuilder()
                .setId("Id1")
                .setTripUpdate(t1)
                .build();
    }

}
