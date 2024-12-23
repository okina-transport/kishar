package org.entur.kishar.gtfsrt;

import com.google.transit.realtime.GtfsRealtime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import uk.org.siri.www.siri.PtSituationElementStructure;

import static org.entur.kishar.gtfsrt.Helper.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class TestAlertFactory {
    AlertFactory alertFactory;

    @BeforeEach
    void init() {
        alertFactory = new AlertFactory();
    }

    @Test
    void testCreateAlertFromSituation() {

        PtSituationElementStructure ptSituation = createPtSituationElement("RUT");


        GtfsRealtime.Alert.Builder alertBuilder = alertFactory.createAlertFromSituation(ptSituation);
        assertNotNull(alertBuilder);
        GtfsRealtime.Alert alert = alertBuilder.build();
        assertNotNull(alert);

        assertAlert(alert);

    }

    static void assertAlert(GtfsRealtime.Alert alert) {
        GtfsRealtime.TranslatedString headerText = alert.getHeaderText();
        assertNotNull(headerText);
        assertEquals(1, headerText.getTranslationCount());
        assertEquals(summaryValue, headerText.getTranslation(0).getText());


        GtfsRealtime.TranslatedString descriptionText = alert.getDescriptionText();
        assertNotNull(descriptionText);
        assertEquals(1, descriptionText.getTranslationCount());
        assertEquals(descriptionValue, descriptionText.getTranslation(0).getText());
    }
}
