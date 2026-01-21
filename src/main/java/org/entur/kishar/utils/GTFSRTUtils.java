package org.entur.kishar.utils;

import com.google.transit.realtime.GtfsRealtime;

public class GTFSRTUtils {

    public static GtfsRealtime.TripUpdate.StopTimeUpdate updateDepartureDelay(GtfsRealtime.TripUpdate.StopTimeUpdate stopTimeUpdate, int newDepartureDelay) {
        int currentStopDelay = 0;
        if (stopTimeUpdate.getDeparture() != null && stopTimeUpdate.getDeparture().hasDelay()){
            currentStopDelay = stopTimeUpdate.getDeparture().getDelay();
        }
        GtfsRealtime.TripUpdate.StopTimeEvent newEvent = stopTimeUpdate.getDeparture().toBuilder().setDelay(currentStopDelay + newDepartureDelay).build();
        return stopTimeUpdate.toBuilder().setDeparture(newEvent).build();
    }

    public static GtfsRealtime.TripUpdate.StopTimeUpdate updateArrivalDelay(GtfsRealtime.TripUpdate.StopTimeUpdate stopTimeUpdate, int newArrivalDelay) {
        int currentStopDelay = 0;
        if (stopTimeUpdate.getArrival() != null && stopTimeUpdate.getArrival().hasDelay()){
            currentStopDelay = stopTimeUpdate.getArrival().getDelay();
        }
        GtfsRealtime.TripUpdate.StopTimeEvent newEvent = stopTimeUpdate.getArrival().toBuilder().setDelay(currentStopDelay + newArrivalDelay).build();
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
