package org.entur.kishar.gtfsrt;

import com.google.transit.realtime.GtfsRealtime;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RedisServiceTest {

    private static final long NOW = 1_790_000_000L;
    private static final long ONE_DAY = 24 * 60 * 60;

    @Test
    void mergedTripUpdateTimeToLiveIsRelativeToNow() {
        GtfsRealtime.TripUpdate tripUpdate = tripUpdate(
                stopTimeUpdate("stop1", NOW + 600, NOW + 660),
                stopTimeUpdate("stop2", NOW + 1200, NOW + 1500));

        long timeToLive = RedisService.computeMergedTripUpdateTimeToLive(tripUpdate, 300, NOW);

        assertEquals(1500, timeToLive);
        assertTrue(timeToLive < ONE_DAY, "TTL must be a duration, not an epoch timestamp");
    }

    @Test
    void mergedTripUpdateTimeToLiveKeepsIncomingWhenGreater() {
        GtfsRealtime.TripUpdate tripUpdate = tripUpdate(stopTimeUpdate("stop1", NOW + 60, NOW + 120));

        assertEquals(900, RedisService.computeMergedTripUpdateTimeToLive(tripUpdate, 900, NOW));
    }

    @Test
    void mergedTripUpdateTimeToLiveKeepsIncomingWhenTripIsInThePast() {
        GtfsRealtime.TripUpdate tripUpdate = tripUpdate(stopTimeUpdate("stop1", NOW - 3600, NOW - 3500));

        assertEquals(300, RedisService.computeMergedTripUpdateTimeToLive(tripUpdate, 300, NOW));
    }

    @Test
    void mergedTripUpdateTimeToLiveKeepsIncomingWhenNoStopTimes() {
        GtfsRealtime.TripUpdate tripUpdate = tripUpdate(
                GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder().setStopId("stop1").build());

        assertEquals(300, RedisService.computeMergedTripUpdateTimeToLive(tripUpdate, 300, NOW));
    }

    private static GtfsRealtime.TripUpdate tripUpdate(GtfsRealtime.TripUpdate.StopTimeUpdate... stopTimeUpdates) {
        GtfsRealtime.TripUpdate.Builder builder = GtfsRealtime.TripUpdate.newBuilder()
                .setTrip(GtfsRealtime.TripDescriptor.newBuilder().setTripId("trip1"));
        for (GtfsRealtime.TripUpdate.StopTimeUpdate stopTimeUpdate : stopTimeUpdates) {
            builder.addStopTimeUpdate(stopTimeUpdate);
        }
        return builder.build();
    }

    private static GtfsRealtime.TripUpdate.StopTimeUpdate stopTimeUpdate(String stopId, long arrival, long departure) {
        return GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder()
                .setStopId(stopId)
                .setArrival(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setTime(arrival))
                .setDeparture(GtfsRealtime.TripUpdate.StopTimeEvent.newBuilder().setTime(departure))
                .build();
    }
}
