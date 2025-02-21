package org.entur.kishar.gtfsrt;


import com.google.transit.realtime.GtfsRealtime;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Value;


import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static org.junit.Assert.assertEquals;

public class TestSiriSMToGtfsRealtimeService extends SiriToGtfsRealtimeServiceTest{

    @Value("${kishar.redis.enabled:false}") boolean redisEnabled;

    @Value("${kishar.redis.host:}") String host;

    @Value("${kishar.redis.port:}") String port;


    @Test
    public void trip_updates_have_been_ordered_on_merge() throws IOException {

        RedisService redisService1 = new RedisService(redisEnabled, host, port);

        ZonedDateTime arrival = ZonedDateTime.of(2055, 8, 20, 12, 0, 0, 0, ZoneOffset.UTC);
        ZonedDateTime departure = ZonedDateTime.of(2055, 8, 20, 12, 0, 30, 0, ZoneOffset.UTC);
        GtfsRealtime.FeedEntity incomingEntity = generateEntity("STOP1", arrival, departure);

        ZonedDateTime arrival2 = ZonedDateTime.of(2055, 8, 20, 12, 10, 0, 0, ZoneOffset.UTC);
        ZonedDateTime departure2 = ZonedDateTime.of(2055, 8, 20, 12, 10, 30, 0, ZoneOffset.UTC);
        GtfsRealtime.FeedEntity existing = generateEntity("STOP2", arrival2, departure2);

        // Simulating that STOP2(12h10) was integrated before. It's the existing Entity.
        // STOP1(12h00) is the new incoming tripUpdate
        GtfsRealtime.TripUpdate mergedTrip = redisService1.buildMergedTripUpdate(incomingEntity, existing);
        System.out.println("mergedTrip: " + mergedTrip);

        // Expected : trip updates have been ordered and STOP1 is before STOP2

        assertEquals(mergedTrip.getStopTimeUpdate(0).getStopId(), "STOP1");
        assertEquals(mergedTrip.getStopTimeUpdate(1).getStopId(), "STOP2");


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

        GtfsRealtime.FeedEntity result = GtfsRealtime.FeedEntity.newBuilder()
                .setId("Id1")
                .setTripUpdate(t1)
                .build();

        return result;
    }

}
