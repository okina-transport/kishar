package org.entur.kishar.routes;

import org.apache.camel.CamelContext;
import org.apache.camel.EndpointInject;
import org.apache.camel.Exchange;
import org.apache.camel.FluentProducerTemplate;
import org.apache.camel.builder.AdviceWith;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.model.language.ConstantExpression;
import org.apache.camel.test.spring.junit5.CamelSpringBootTest;
import org.apache.camel.test.spring.junit5.MockEndpoints;
import org.apache.camel.test.spring.junit5.UseAdviceWith;
import org.entur.kishar.config.SubscriptionConfig;
import org.entur.kishar.utils.IdProcessingParameters;
import org.entur.kishar.utils.ObjectType;
import org.entur.kishar.utils.TestUtils;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;

import java.util.Map;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

@SpringBootTest
@CamelSpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@UseAdviceWith
// direct: ignored to avoid initialization failure
@MockEndpoints("direct:")
class SynchronizationRouteTest {

    @EndpointInject("mock:mockTimerEndpoint")
    protected MockEndpoint mockEndpointTimer;
    @Autowired
    private CamelContext camelContext;
    @Autowired
    private SubscriptionConfig subscriptionConfig;
    @Autowired
    private FluentProducerTemplate fluentProducerTemplate;

    @Test
    void testIshtarSynchronizationRoute() throws Exception {
        AdviceWith.adviceWith(camelContext, "ISHTAR_SYNCHRONIZATION_ROUTE", routeBuilder -> {
            routeBuilder.replaceFromWith("direct:mockTimerEndpoint");

            routeBuilder
                    .weaveById("ishtarHttpGet")
                    .replace()
                    .setBody(new ConstantExpression(TestUtils.readMockResponseFromFile("src/test/resources/mock/mock_id_processing_parameters.json")));
        });

        camelContext.start();

        Exchange exchange = fluentProducerTemplate.to("direct:mockTimerEndpoint").request(Exchange.class);

        mockEndpointTimer.assertIsSatisfied();

        assertThat(exchange).isNotNull();
        assertThat(exchange.getIn().hasHeaders()).isTrue();
        assertThat(exchange.getIn().getHeader("Accept")).isEqualTo("application/json");
        assertThat(exchange.getIn().getHeader("ishtarIdProcessingParametersResource")).isEqualTo("http://ishtar.api/resource?dataType=gtfs-rt");
        Map<ObjectType, IdProcessingParameters> idProcessingParameters = subscriptionConfig.getIdParametersForDataset("STAS");
        if (!idProcessingParameters.isEmpty()) {
            IdProcessingParameters ipp = idProcessingParameters.get(ObjectType.STOP);
            assertThat(ipp.getDatasetId()).isEqualTo("STAS");
            assertThat(ipp.getInputPrefixToRemove()).isEqualTo("STAS:StopPoint:BP:");
            assertThat(ipp.getInputSuffixToRemove()).isEqualTo(":LOC");
            assertThat(ipp.getOutputPrefixToAdd()).isEqualTo("STAS:Quay:");
            assertThat(ipp.getOutputSuffixToAdd()).isEmpty();
        } else {
            fail("IdProcessingParameters not found");
        }

    }
}
