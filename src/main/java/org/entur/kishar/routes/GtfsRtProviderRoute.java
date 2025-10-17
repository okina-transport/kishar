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

import org.entur.kishar.gtfsrt.GtfsTripsService;
import org.entur.kishar.gtfsrt.SiriToGtfsRealtimeService;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;

@Service
@Configuration
public class GtfsRtProviderRoute extends RestRouteBuilder {
    public static final int GTFS_TRIPS_LOADING_PERIOD_MS = 30 * 60 * 1000; // 30 minutes
    public static final String PARAM_DATASET_ID = "datasetId";

    private final SiriToGtfsRealtimeService siriToGtfsRealtimeService;
    private final GtfsTripsService gtfsTripsService;

    public GtfsRtProviderRoute(SiriToGtfsRealtimeService siriToGtfsRealtimeService, GtfsTripsService gtfsTripsService) {
        this.siriToGtfsRealtimeService = siriToGtfsRealtimeService;
        this.gtfsTripsService = gtfsTripsService;
    }

    @Override
    public void configure() {

        super.configure();

        rest("/api/")
                .get("trip-updates/{" + PARAM_DATASET_ID + "}").to("direct:getTripUpdates").produces("application/octet-stream").id("kishar.trip-updates")
                .get("vehicle-positions/{" + PARAM_DATASET_ID + "}").to("direct:getVehiclePositions").produces("application/octet-stream").id("kishar.vehicle-positions")
                .get("alerts/{" + PARAM_DATASET_ID + "}").to("direct:getAlerts").produces("application/octet-stream").id("kishar.alerts")
                .get("debug/status").to("direct:getStatus").produces("application/text").id("kishar.status")
                .get("debug/reset").to("direct:reset").produces("application/text").id("kishar.status")
                .delete("dataset/{" + PARAM_DATASET_ID + "}").to("direct:clearCacheByDatasetId").id("kishar.clearCacheByDatasetId")
        ;


        from("direct:getStatus")
                .routeId("kishar.getStatus")
                .bean(siriToGtfsRealtimeService, "getStatus()")
        ;


        from("direct:reset")
                .routeId("kishar.reset")
                .bean(siriToGtfsRealtimeService, "reset()")
        ;

        from("direct:getTripUpdates")
                .routeId("kishar.getTripUpdates")
                .choice()
                .when(header("useOriginalId").isNotNull())
                .setHeader("useOriginalId", header("useOriginalId"))
                .otherwise()
                .setHeader("useOriginalId", constant(false))
                .end()
                .bean(siriToGtfsRealtimeService, "getTripUpdates(${header.Content-Type},${header.datasetId},${header.useOriginalId})")
                .setHeader("Content-Disposition", constant("attachment; filename=trip-updates.pbf"))
                .setHeader("Content-Type", constant("application/octet-stream"))
        ;

        from("direct:getVehiclePositions")
                .routeId("kishar.getVehiclePositions")
                .bean(siriToGtfsRealtimeService, "getVehiclePositions(${header.Content-Type},${header.datasetId},${header.useOriginalId})")
                .setHeader("Content-Disposition", constant("attachment; filename=vehicle-positions.pbf"))
                .setHeader("Content-Type", constant("application/octet-stream"))
        ;

        from("direct:getAlerts")
                .routeId("kishar.getAlerts")
                .choice()
                .when(header("useOriginalId").isNotNull())
                .setHeader("useOriginalId", header("useOriginalId"))
                .otherwise()
                .setHeader("useOriginalId", constant(false))
                .end()
                .bean(siriToGtfsRealtimeService, "getAlerts(${header.Content-Type},${header.datasetId},${header.useOriginalId})")
                .setHeader("Content-Disposition", constant("attachment; filename=alerts.pbf"))
                .setHeader("Content-Type", constant("application/octet-stream"))
        ;

        from("direct:clearCacheByDatasetId")
                .routeId("kishar.clearCacheByDatasetId")
                .process(e -> siriToGtfsRealtimeService.clearCacheByDatasetId(e.getIn().getHeader(PARAM_DATASET_ID, String.class)))
        ;

        from("timer://kishar.update.output?fixedRate=true&period=10s")
                .bean(siriToGtfsRealtimeService, "writeOutput()")
                .routeId("kishar.update.output")
        ;

        from("timer://kishar.load.gtfs.trips?fixedRate=true&delay=0&period=" + GTFS_TRIPS_LOADING_PERIOD_MS)
                .process(e -> gtfsTripsService.loadTripsFromFileSystem())
                .routeId("kishar.load.gtfs.trips")
        ;
    }
}
