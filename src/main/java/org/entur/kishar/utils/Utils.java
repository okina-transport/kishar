package org.entur.kishar.utils;

import org.entur.kishar.utils.subscription.SubscriptionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Service
public class Utils {

    private static final Logger logger = LoggerFactory.getLogger(Utils.class);

    private final SubscriptionConfig subscriptionConfig;

    @Autowired
    public Utils(SubscriptionConfig subscriptionConfig) {
        this.subscriptionConfig = subscriptionConfig;
    }

    /**
     * Builds a map with key = datasetId and value = idProcessingParams for this dataset and objectType = stop
     *
     * @param dataset
     * @return
     */
    public Map<String, IdProcessingParameters> buildIdProcessingMap(String dataset, ObjectType objectType) {

        Map<String, IdProcessingParameters> resultMap = new HashMap<>();
        Optional<IdProcessingParameters> idParamsOpt = subscriptionConfig.getIdParametersForDataset(dataset, objectType);
        idParamsOpt.ifPresent(idParams -> resultMap.put(dataset, idParams));

        return resultMap;
    }

}
