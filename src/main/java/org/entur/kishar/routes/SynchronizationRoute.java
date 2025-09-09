package org.entur.kishar.routes;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.builder.RouteBuilder;
import org.apache.commons.collections4.CollectionUtils;
import org.entur.kishar.config.SubscriptionConfig;
import org.entur.kishar.config.TokenService;
import org.entur.kishar.utils.IdProcessingParameters;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Configuration
@Slf4j
public class SynchronizationRoute extends RouteBuilder {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    static {
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private final SubscriptionConfig subscriptionConfig;
    private final TokenService tokenService;
    private final String ishtarUrl;
    private final String ishtarSynchronizationInterval;
    private final String ishtarSynchronizationInitialDelay;

    public SynchronizationRoute(SubscriptionConfig subscriptionConfig,
                                TokenService tokenService,
                                @Value("${ishtar.server.url}") String ishtarUrl,
                                // default value : 5 minutes
                                @Value("${ishtar.synchronisation.interval:300000}") String ishtarSynchronizationInterval,
                                // default value 5 seconds
                                @Value("${ishtar.synchronisation.initialDelay:5000}") String ishtarSynchronizationInitialDelay) {
        this.subscriptionConfig = subscriptionConfig;
        this.tokenService = tokenService;
        this.ishtarUrl = ishtarUrl;
        this.ishtarSynchronizationInterval = ishtarSynchronizationInterval;
        this.ishtarSynchronizationInitialDelay = ishtarSynchronizationInitialDelay;
    }

    @Override
    public void configure() {
        from("timer://ishtarSynchronization?period=" + ishtarSynchronizationInterval + "&delay=" + ishtarSynchronizationInitialDelay)
                .routeId("ISHTAR_SYNCHRONIZATION_ROUTE")
                .setHeader("ishtarIdProcessingParametersResource", simple(ishtarUrl+"?dataType=gtfs-rt"))
                .setHeader("Accept", constant("application/json"))
                .setHeader("Authorization", method(tokenService, "getAuthorizationHeader"))
                .toD("${header.ishtarIdProcessingParametersResource}")
                .id("ishtarHttpGet")
                .process(exchange -> {
                    log.info("Get id processing parameters from ISHTAR");
                    String rawJson = exchange.getIn().getBody(String.class);
                    List<IdProcessingParameters> ishtarIdProcessingParametersConfiguration = objectMapper.readValue(rawJson, new TypeReference<>() {
                    });
                    log.info("Retrieved {} id processing parameters from ISHTAR", CollectionUtils.size(ishtarIdProcessingParametersConfiguration));
                    subscriptionConfig.mergeIdProcessingParams(ishtarIdProcessingParametersConfiguration);
                });
    }
}
