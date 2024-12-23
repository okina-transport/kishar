package org.entur.kishar.gtfsrt.mappers;


import org.entur.kishar.utils.IdProcessingParameters;
import org.entur.kishar.utils.ObjectType;
import org.entur.kishar.utils.Utils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class IdMapperTest {

    private static final String DATASET = "TST";

    private static final String STOP_ID = "PTIS";

    private static final String OUTPUT_PREFIX_TO_ADD = "NAOLIBORG:StopPlace:";

    private static final String OUTPUT_SUFFIX_TO_ADD = ":LOC";

    @InjectMocks
    private IdMapper idMapper;

    @Mock
    private Utils utils;

    @Test
    void applyIdProcessingParameters_noIdProcessingParametersForDataSet_test() {
        when(utils.buildIdProcessingMap(DATASET, ObjectType.STOP)).thenReturn(Collections.emptyMap());

        String result = idMapper.applyIdProcessingParameters(DATASET, STOP_ID);

        assertThat(result).isEqualTo(STOP_ID);
    }

    @Test
    void applyIdProcessingParameters_processingParametersFound_test() {
        IdProcessingParameters idProcessingParameters = new IdProcessingParameters();
        idProcessingParameters.setOutputPrefixToAdd(OUTPUT_PREFIX_TO_ADD);
        idProcessingParameters.setOutputSuffixToAdd(OUTPUT_SUFFIX_TO_ADD);
        when(utils.buildIdProcessingMap(DATASET, ObjectType.STOP)).thenReturn(Map.of(DATASET, idProcessingParameters));

        String result = idMapper.applyIdProcessingParameters(DATASET, STOP_ID);

        assertThat(result).isEqualTo(OUTPUT_PREFIX_TO_ADD+STOP_ID+OUTPUT_SUFFIX_TO_ADD);
    }
}