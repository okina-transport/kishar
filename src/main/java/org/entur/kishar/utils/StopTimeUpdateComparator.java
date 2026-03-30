package org.entur.kishar.utils;

import com.google.transit.realtime.GtfsRealtime;

import java.util.Comparator;


public class StopTimeUpdateComparator implements Comparator<GtfsRealtime.TripUpdate.StopTimeUpdate> {
    @Override
    public int compare(GtfsRealtime.TripUpdate.StopTimeUpdate stu1, GtfsRealtime.TripUpdate.StopTimeUpdate stu2) {
        if (stu2 == null) {
            return -1;
        }

        if (stu1.hasStopSequence() && stu2.hasStopSequence()) {
            return Integer.compare(stu1.getStopSequence(), stu2.getStopSequence());
        }

        if ( stu1.getDeparture() != null && stu1.getDeparture().hasTime() && stu2.getDeparture() != null && stu2.getDeparture().hasTime()) {
            return Long.compare(stu1.getDeparture().getTime(), stu2.getDeparture().getTime());
        }

        if (stu1.getArrival() != null && stu1.getArrival().hasTime() &&  stu2.getArrival() != null && stu2.getArrival().hasTime()) {
            return Long.compare(stu1.getArrival().getTime(), stu2.getArrival().getTime());
        }


        return stu1.getStopId().compareTo(stu2.getStopId());
    }
}