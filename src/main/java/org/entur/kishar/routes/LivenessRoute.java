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

import org.apache.camel.Exchange;
import org.springframework.stereotype.Service;

@Service
public class LivenessRoute extends RestRouteBuilder {

    @Override
    public void configure() throws Exception {

        rest("health/").tag("health")
                .get("ready").to("direct:ready")
                .get("up").to("direct:up")
                .get("healthy").to("direct:healthy")
                .get("favicon.ico").to("direct:notfound")
        ;

        //To avoid large stacktraces in the log when fetching data using browser
        from("direct:notfound")
                .setHeader(Exchange.HTTP_RESPONSE_CODE, constant("404"))
                .routeId("health.notfound")
        ;

        // Application is ready to accept traffic
        from("direct:ready")
                .setHeader(Exchange.HTTP_RESPONSE_CODE, constant("200"))
                .setBody(constant("OK"))
                .routeId("health.ready")
        ;

        // Application is (still) alive and well
        from("direct:up")
                .setBody(simple("OK"))
                .setHeader(Exchange.HTTP_RESPONSE_CODE, constant("200"))
                .routeId("health.up")
        ;
    }

}
