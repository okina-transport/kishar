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
import org.entur.kishar.utils.IdProcessingParameters;
import org.entur.kishar.utils.ObjectType;
import org.entur.kishar.utils.TestUtils;
import org.entur.kishar.utils.subscription.SubscriptionConfig;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;

import java.util.Optional;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

@SpringBootTest
@CamelSpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@UseAdviceWith
// direct: ignored to avoid initialization failure
@MockEndpoints("direct:")
class SynchronizationRouteTest {

    @Autowired
    private CamelContext camelContext;

    @Autowired
    private SubscriptionConfig subscriptionConfig;

    @EndpointInject("mock:mockTimerEndpoint")
    protected MockEndpoint mockEndpointTimer;

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
        assertThat(exchange.getIn().getHeader("ishtarIdProcessingParametersResource")).isEqualTo("http://ishtar.api/resource");
        Optional<IdProcessingParameters> parameters = subscriptionConfig.getIdParametersForDataset("STAS", ObjectType.STOP);
        if (parameters.isPresent()) {
            IdProcessingParameters idProcessingParameters = parameters.get();
            assertThat(idProcessingParameters.getDatasetId()).isEqualTo("STAS");
            assertThat(idProcessingParameters.getInputPrefixToRemove()).isEqualTo("STAS:StopPoint:BP:");
            assertThat(idProcessingParameters.getInputSuffixToRemove()).isEqualTo(":LOC");
            assertThat(idProcessingParameters.getOutputPrefixToAdd()).isEqualTo("STAS:Quay:");
            assertThat(idProcessingParameters.getOutputSuffixToAdd()).isEmpty();
        } else {
            fail("IdProcessingParameters not found");
        }

    }
}
