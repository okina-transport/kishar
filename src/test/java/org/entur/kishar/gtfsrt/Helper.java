package org.entur.kishar.gtfsrt;

import com.google.protobuf.Timestamp;
import uk.org.siri.www.siri.*;

import java.time.Instant;

public class Helper {

    static String situationNumberValue = "TST:SituationNumber:1234";
    static String summaryValue = "Situation summary";
    static String descriptionValue = "Situation description";

    static PtSituationElementStructure createPtSituationElement(String datasource) {

        EntryQualifierStructure situationNumber = EntryQualifierStructure.newBuilder()
                .setValue(situationNumberValue)
                .build();

        DefaultedTextStructure summary = DefaultedTextStructure.newBuilder()
                .setValue(summaryValue)
                .build();

        DefaultedTextStructure description = DefaultedTextStructure.newBuilder()
                .setValue(descriptionValue)
                .build();

        ParticipantRefStructure requestorRef = ParticipantRefStructure.newBuilder()
                .setValue(datasource)
                .build();

        PtSituationElementStructure.InfoLinksType infoLinksType = PtSituationElementStructure.InfoLinksType
            .newBuilder()
            .addInfoLink(InfoLinkStructure.newBuilder()
                .setUri("http://www.example.com")
                .build()
            )
            .build();

        Instant time = Instant.now();
        Timestamp startTimestamp = Timestamp.newBuilder().setSeconds(time.getEpochSecond() - 1000).build();
        Timestamp endTimestamp = Timestamp.newBuilder().setSeconds(time.getEpochSecond() + 1000).build();

        HalfOpenTimestampOutputRangeStructure halfOpenTimestampOutputRangeStructure = HalfOpenTimestampOutputRangeStructure
                .newBuilder()
                .setStartTime(startTimestamp)
                .setEndTime(endTimestamp)
                .build();


        return PtSituationElementStructure.newBuilder()
                .setSituationNumber(situationNumber)
                .addSummary(summary)
                .addDescription(description)
                .setParticipantRef(requestorRef)
                .setInfoLinks(infoLinksType)
                .addValidityPeriod(halfOpenTimestampOutputRangeStructure)
                .build();
    }

    static FramedVehicleJourneyRefStructure createFramedVehicleJourneyRefStructure(String datedVehicleJourneyRef) {
        if (datedVehicleJourneyRef == null) {
            return null;
        }

        DataFrameRefStructure dataFrameRef = DataFrameRefStructure.newBuilder()
                .setValue("2018-12-12")
                .build();

        return FramedVehicleJourneyRefStructure.newBuilder()
                .setDatedVehicleJourneyRef(datedVehicleJourneyRef)
                .setDataFrameRef(dataFrameRef)
                .build();
    }

    static LineRefStructure createLineRef(String lineRefValue) {
        return LineRefStructure.newBuilder()
                .setValue(lineRefValue)
                .build();
    }

    static PtSituationElementStructure createPtSituationElementWithoutEndDateValidityPeriod(String datasource) {

        EntryQualifierStructure situationNumber = EntryQualifierStructure.newBuilder()
                .setValue(situationNumberValue)
                .build();

        DefaultedTextStructure summary = DefaultedTextStructure.newBuilder()
                .setValue(summaryValue)
                .build();

        DefaultedTextStructure description = DefaultedTextStructure.newBuilder()
                .setValue(descriptionValue)
                .build();

        ParticipantRefStructure requestorRef = ParticipantRefStructure.newBuilder()
                .setValue(datasource)
                .build();

        PtSituationElementStructure.InfoLinksType infoLinksType = PtSituationElementStructure.InfoLinksType
                .newBuilder()
                .addInfoLink(InfoLinkStructure.newBuilder()
                        .setUri("http://www.example.com")
                        .build()
                )
                .build();

        Instant time = Instant.now();
        Timestamp startTimestamp = Timestamp.newBuilder().setSeconds(time.getEpochSecond() - 1000).build();

        HalfOpenTimestampOutputRangeStructure halfOpenTimestampOutputRangeStructure = HalfOpenTimestampOutputRangeStructure
                .newBuilder()
                .setStartTime(startTimestamp)
                .build();


        return PtSituationElementStructure.newBuilder()
                .setSituationNumber(situationNumber)
                .addSummary(summary)
                .addDescription(description)
                .setParticipantRef(requestorRef)
                .setInfoLinks(infoLinksType)
                .addValidityPeriod(halfOpenTimestampOutputRangeStructure)
                .build();
    }
}
