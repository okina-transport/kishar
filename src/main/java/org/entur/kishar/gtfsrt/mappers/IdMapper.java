package org.entur.kishar.gtfsrt.mappers;

import org.entur.kishar.utils.IdProcessingParameters;
import org.entur.kishar.utils.ObjectType;
import org.entur.kishar.utils.Utils;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class IdMapper {

    private final Utils utils;

    public IdMapper(Utils utils) {
        this.utils = utils;
    }

    /**
     * Extract a stopId from a subscriptionSetup and transforms it, with idProcessingParams
     *
     * @return the transformed stop id
     */
    public String applyIdProcessingParameters(String datasetId, String stopId) {
        Map<String, IdProcessingParameters> idProcessingMap = utils.buildIdProcessingMap(datasetId, ObjectType.STOP);

        return idProcessingMap.containsKey(datasetId) ? idProcessingMap.get(datasetId).applyTransformationToString(stopId) : stopId;
    }

}
