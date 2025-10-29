package org.entur.kishar.gtfsrt;


import com.google.transit.realtime.GtfsRealtime;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;


import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static org.junit.Assert.assertEquals;

class TestSiriSMToGtfsRealtimeService extends SiriToGtfsRealtimeServiceTest{

    @Autowired
    RedisService tested;


    @Test
    void test_delay_is_applied_to_successors()  {


        ZonedDateTime arrival = ZonedDateTime.of(2055, 8, 20, 12, 0, 0, 0, ZoneOffset.UTC);
        ZonedDateTime departure = ZonedDateTime.of(2055, 8, 20, 12, 0, 30, 0, ZoneOffset.UTC);
        GtfsRealtime.FeedEntity incomingEntity = generateEntityWithDelay("STOP1", arrival, departure, 40);

        ZonedDateTime arrival2 = ZonedDateTime.of(2055, 8, 20, 12, 1, 0, 0, ZoneOffset.UTC);
        ZonedDateTime departure2 = ZonedDateTime.of(2055, 8, 20, 12, 1, 30, 0, ZoneOffset.UTC);
        GtfsRealtime.FeedEntity existing = generateEntity("STOP2", arrival2, departure2);


        // Simulating that STOP2(12h10) was integrated before. It's the existing Entity.
        // STOP1(12h00) is the new incoming tripUpdate
        GtfsRealtime.TripUpdate mergedTrip = tested.buildMergedTripUpdate(incomingEntity, existing);


        // Expected : time of stop 2 has been updated with delay from stop 1
        Assertions.assertTrue(mergedTrip.getStopTimeUpdate(1).getArrival().hasDelay());
        Assertions.assertTrue(mergedTrip.getStopTimeUpdate(1).getDeparture().hasDelay());

        // 40s delay from STOP1 has been applied to stop 2
        Assertions.assertEquals(40, mergedTrip.getStopTimeUpdate(1).getArrival().getDelay());
        Assertions.assertEquals(40, mergedTrip.getStopTimeUpdate(1).getDeparture().getDelay());

        // arrival for stop2 is now 12h01 40s and departure : 12h02 10s
        Assertions.assertEquals(2702376100L, mergedTrip.getStopTimeUpdate(1).getArrival().getTime());
        Assertions.assertEquals(2702376130L, mergedTrip.getStopTimeUpdate(1).getDeparture().getTime());


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
