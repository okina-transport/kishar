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
package org.entur.kishar.routes;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.dataformat.JaxbDataFormat;
import org.entur.kishar.gtfsrt.SiriToGtfsRealtimeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
@Configuration
public class SiriIncomingRoute extends RouteBuilder {

    private SiriToGtfsRealtimeService siriToGtfsRealtimeService;

    @Value("${kishar.anshar.polling.url.et}")
    private String ansharUrlEt;
    @Value("${kishar.anshar.polling.url.vm}")
    private String ansharUrlVm;
    @Value("${kishar.anshar.polling.url.sx}")
    private String ansharUrlSx;

    @Value("${kishar.anshar.polling.interval.sec}")
    private int pollingIntervalSec;



    @Value("${kishar.anshar.activemq.topic.enabled:false}")
    private boolean activeMqTopicEnabled;

    @Value("${kishar.anshar.activemq.topic.name.et}")
    private String activeMqTopicEt;
    @Value("${kishar.anshar.activemq.topic.name.vm}")
    private String activeMqTopicVm;
    @Value("${kishar.anshar.activemq.topic.name.sx}")
    private String activeMqTopicSx;

    public SiriIncomingRoute(@Autowired SiriToGtfsRealtimeService siriToGtfsRealtimeService) {
        this.siriToGtfsRealtimeService = siriToGtfsRealtimeService;
    }

    @Override
    public void configure() throws Exception {

        JaxbDataFormat dataFormatType = new JaxbDataFormat();

        if (activeMqTopicEnabled) {

            from(activeMqTopicVm)
                    .log("Incoming SIRI from " + activeMqTopicVm)
                    .to("direct:process.helpers.xml")
                    .routeId("kishar.activemq.topic.vm")
            ;

            from(activeMqTopicEt)
                    .log("Incoming SIRI from " + activeMqTopicEt)
                    .to("direct:process.helpers.xml")
                    .routeId("kishar.activemq.topic.et")
            ;

            from(activeMqTopicSx)
                    .log("Incoming SIRI from " + activeMqTopicSx)
                    .to("direct:process.helpers.xml")
                    .routeId("kishar.activemq.topic.sx")
            ;
        } else {

            from("quartz2://kishar.polling_vm?fireNow=true&trigger.repeatInterval=" + pollingIntervalSec * 1000)
                    .to("http4://" + ansharUrlVm + "?requestorId=kishar-" + UUID.randomUUID())
                    .to("direct:process.helpers.xml")
                    .routeId("kishar.polling.vm")
            ;
            from("quartz2://kishar.polling_et?fireNow=true&trigger.repeatInterval=" + pollingIntervalSec * 1000)
                    .to("http4://" + ansharUrlEt + "?requestorId=kishar-" + UUID.randomUUID())
                    .to("direct:process.helpers.xml")
                    .routeId("kishar.polling.et")
            ;
            from("quartz2://kishar.polling_sx?fireNow=true&trigger.repeatInterval=" + pollingIntervalSec * 1000)
                    .to("http4://" + ansharUrlSx + "?requestorId=kishar-" + UUID.randomUUID())
                    .to("direct:process.helpers.xml")
                    .routeId("kishar.polling.sx")
            ;
        }

        from("direct:process.helpers.xml")
                .marshal(dataFormatType)
                .bean(siriToGtfsRealtimeService, "processDelivery(${body})")
                ;

    }
}