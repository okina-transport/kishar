package org.entur.kishar.routes;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.builder.RouteBuilder;
import org.entur.kishar.config.TokenService;
import org.entur.kishar.utils.IdProcessingParameters;
import org.entur.kishar.utils.subscription.SubscriptionConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Configuration
public class SynchronizationRoute extends RouteBuilder {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    static {
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Value("${ishtar.server.url}")
    private String ishtarUrl;

    // default value : 5 minutes
    @Value("${ishtar.synchronisation.interval:300000}")
    private String ishtarSynchronizationInterval;

    // default value 5 seconds
    @Value("${ishtar.synchronisation.interval:5000}")
    private String ishtarSynchronizationInitialDelay;

    @Autowired
    private TokenService tokenService;

    private final SubscriptionConfig subscriptionConfig;

    public SynchronizationRoute(SubscriptionConfig subscriptionConfig) {
        this.subscriptionConfig = subscriptionConfig;
    }

    @Override
    public void configure() throws Exception {
        from("timer://ishtarSynchronization?period=" + ishtarSynchronizationInterval + "&delay=" + ishtarSynchronizationInitialDelay)
                .routeId("ISHTAR_SYNCHRONIZATION_ROUTE")
                .setHeader("ishtarIdProcessingParametersResource", constant(ishtarUrl))
                .setHeader("Accept", constant("application/json"))
                .setHeader("Authorization", constant("Bearer " + tokenService.getToken()))
                .toD("${header.ishtarIdProcessingParametersResource}")
                .id("ishtarHttpGet")
                .process(exchange -> {
                    String rawJson = exchange.getIn().getBody(String.class);
                    List<IdProcessingParameters> ishtarIdProcessingParametersConfiguration = objectMapper.readValue(rawJson, new TypeReference<>() {
                    });
                    subscriptionConfig.mergeIdProcessingParams(ishtarIdProcessingParametersConfiguration);
                });
    }
}
