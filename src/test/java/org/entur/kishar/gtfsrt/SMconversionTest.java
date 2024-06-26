package org.entur.kishar.gtfsrt;

import com.google.protobuf.Timestamp;
import com.google.transit.realtime.GtfsRealtime;
import org.entur.kishar.gtfsrt.mappers.GtfsRtMapper;
import org.junit.jupiter.api.Test;

import uk.org.siri.www.siri.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static junit.framework.TestCase.assertEquals;

public class SMconversionTest {

    @Test
    public void testSMwithoutTripId() throws IOException {
        GtfsRtMapper gtfsMapper =  new GtfsRtMapper(null, null, 0, 0);

        MonitoredStopVisitStructure.Builder monStructBuild = MonitoredStopVisitStructure.newBuilder();
        MonitoredVehicleJourneyStructure.Builder monVehJourneyBuild = MonitoredVehicleJourneyStructure.newBuilder();
        uk.org.siri.www.siri.LineRefStructure.Builder lineRefBuild = LineRefStructure.newBuilder();
        lineRefBuild.setValue("LineA");
        monVehJourneyBuild.setLineRef(lineRefBuild);
        NaturalLanguageStringStructure.Builder directionNameBuild = NaturalLanguageStringStructure.newBuilder();
        directionNameBuild.setValue("1");
        monVehJourneyBuild.addDirectionName(directionNameBuild);
        MonitoredCallStructure.Builder monitoredCallBuilder = MonitoredCallStructure.newBuilder();
        Timestamp.Builder timeStamp = Timestamp.newBuilder();

        timeStamp.setSeconds(1719387133);
        monitoredCallBuilder.setAimedArrivalTime(timeStamp);
        monVehJourneyBuild.setMonitoredCall(monitoredCallBuilder);
        monStructBuild.setMonitoredVehicleJourney(monVehJourneyBuild);

        GtfsRealtime.TripUpdate.Builder result = gtfsMapper.mapTripUpdateFromStopVisit("TEST", monStructBuild.build());
        assertEquals("",result.getTrip().getTripId());
        assertEquals("LineA",result.getTrip().getRouteId());
        assertEquals(1,result.getTrip().getDirectionId());
        assertEquals("20240626",result.getTrip().getStartDate());
        assertEquals("09:32:13",result.getTrip().getStartTime());
    }

}
