package org.entur.kishar.utils.subscription;

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
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

@PropertySource(value = "${kishar.subscriptions.config.path}", factory = YamlPropertySourceFactory.class)
@ConfigurationProperties(prefix = "anshar")
@Configuration
public class SubscriptionConfig {

    private List<IdProcessingParameters> idProcessingParameters = new CopyOnWriteArrayList<>();

    public Optional<IdProcessingParameters> getIdParametersForDataset(String datasetId, ObjectType objectType) {
        for (IdProcessingParameters idProcessingParametrer : idProcessingParameters) {
            if (datasetId != null && datasetId.equalsIgnoreCase(idProcessingParametrer.getDatasetId()) && objectType != null && objectType.equals(idProcessingParametrer.getObjectType())) {
                return Optional.of(idProcessingParametrer);
            }
        }
        return Optional.empty();
    }

}

