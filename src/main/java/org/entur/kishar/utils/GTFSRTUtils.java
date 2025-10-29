package org.entur.kishar.utils;

import com.google.transit.realtime.GtfsRealtime;

public class GTFSRTUtils {

    public static GtfsRealtime.TripUpdate.StopTimeUpdate updateDepartureDelay(GtfsRealtime.TripUpdate.StopTimeUpdate stopTimeUpdate, int newDepartureDelay) {
        GtfsRealtime.TripUpdate.StopTimeEvent newEvent = stopTimeUpdate.getDeparture().toBuilder().setDelay(newDepartureDelay).build();
        return stopTimeUpdate.toBuilder().setDeparture(newEvent).build();
    }

    public static GtfsRealtime.TripUpdate.StopTimeUpdate updateArrivalDelay(GtfsRealtime.TripUpdate.StopTimeUpdate stopTimeUpdate, int newArrivalDelay) {
        GtfsRealtime.TripUpdate.StopTimeEvent newEvent = stopTimeUpdate.getArrival().toBuilder().setDelay(newArrivalDelay).build();
        return stopTimeUpdate.toBuilder().setArrival(newEvent).build();
    }

    public static GtfsRealtime.TripUpdate.StopTimeUpdate updateDepartureTime(GtfsRealtime.TripUpdate.StopTimeUpdate stopTimeUpdate, long newDepartureTime) {
        GtfsRealtime.TripUpdate.StopTimeEvent newEvent = stopTimeUpdate.getDeparture().toBuilder().setTime(newDepartureTime).build();
        return stopTimeUpdate.toBuilder().setDeparture(newEvent).build();
    }

    public static GtfsRealtime.TripUpdate.StopTimeUpdate updateArrivalTime(GtfsRealtime.TripUpdate.StopTimeUpdate stopTimeUpdate, long newArrivalTime) {
        GtfsRealtime.TripUpdate.StopTimeEvent newEvent = stopTimeUpdate.getArrival().toBuilder().setTime(newArrivalTime).build();
        return stopTimeUpdate.toBuilder().setArrival(newEvent).build();
    }
}
