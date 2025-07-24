package org.entur.kishar.config;

/*
 * Licensed under the EUPL, Version 1.2 or – as soon they will be approved by
 * the European Commission - subsequent versions of the EUPL (the "Licence");
 * You may not use this work except in compliance with the Licence.
 * You may obtain a copy of the Licence at:
 *
 *   https://joinup.ec.europa.eu/software/page/eupl
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the Licence is distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Licence for the specific language governing permissions and
 * limitations under the Licence.
 */

import org.entur.kishar.utils.IdProcessingParameters;
import org.entur.kishar.utils.ObjectType;
import org.entur.kishar.utils.YamlPropertySourceFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

@PropertySource(value = "${kishar.subscriptions.config.path}", factory = YamlPropertySourceFactory.class)
@ConfigurationProperties(prefix = "anshar")
@Configuration
public class SubscriptionConfig {

    private List<IdProcessingParameters> idProcessingParameters = new CopyOnWriteArrayList<>();

    public Map<ObjectType, IdProcessingParameters> getIdParametersForDataset(String datasetId) {
        return idProcessingParameters.stream()
                .filter(ipp -> ipp.getDatasetId().equalsIgnoreCase(datasetId))
                .collect(Collectors.toMap(IdProcessingParameters::getObjectType, p -> p));
    }

    public void setIdProcessingParameters(List<IdProcessingParameters> idProcessingParameters) {
        this.idProcessingParameters = idProcessingParameters;
    }

    public void mergeIdProcessingParams(List<IdProcessingParameters> incomingParams) {
        for (IdProcessingParameters incomingParam : incomingParams) {
            Optional<IdProcessingParameters> existingOpt = getExistingIdProc(incomingParam);
            if (existingOpt.isPresent()) {
                IdProcessingParameters existingIdProc = existingOpt.get();
                existingIdProc.setInputPrefixToRemove(incomingParam.getInputPrefixToRemove());
                existingIdProc.setInputSuffixToRemove(incomingParam.getInputSuffixToRemove());
                existingIdProc.setOutputPrefixToAdd(incomingParam.getOutputPrefixToAdd());
                existingIdProc.setOutputSuffixToAdd(incomingParam.getOutputSuffixToAdd());
            } else {
                idProcessingParameters.add(incomingParam);
            }
        }
    }

    private Optional<IdProcessingParameters> getExistingIdProc(IdProcessingParameters incomingParam) {
        for (IdProcessingParameters idProcessingParameter : idProcessingParameters) {
            if (idProcessingParameter.getDatasetId().equals(incomingParam.getDatasetId()) &&
                    idProcessingParameter.getObjectType().equals(incomingParam.getObjectType())) {
                return Optional.of(idProcessingParameter);
            }
        }
        return Optional.empty();
    }
}

