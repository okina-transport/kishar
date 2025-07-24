package org.entur.kishar.gtfsrt.mappers;

import com.google.transit.realtime.GtfsRealtime;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.entur.kishar.config.SubscriptionConfig;
import org.entur.kishar.gtfsrt.RedisService;
import org.entur.kishar.utils.IdProcessingParameters;
import org.entur.kishar.utils.ObjectType;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@Slf4j
public class IdMapper {

    private final SubscriptionConfig subscriptionConfig;
    private final RedisService redisService;

    public IdMapper(SubscriptionConfig subscriptionConfig, RedisService redisService) {
        this.subscriptionConfig = subscriptionConfig;
        this.redisService = redisService;
    }

    public GtfsRealtime.FeedMessage applyIdProcessingParameters(GtfsRealtime.FeedMessage feedMessage,
                                                                String datasetId, boolean useOriginalId) {
        Map<ObjectType, IdProcessingParameters> otToIpp = subscriptionConfig.getIdParametersForDataset(datasetId);
        GtfsRealtime.FeedMessage.Builder builder = feedMessage.toBuilder();
        for (GtfsRealtime.FeedEntity.Builder feedEntityBuilder : builder.getEntityBuilderList()) {
            applyIdProcessingParameters(feedEntityBuilder, MapUtils.emptyIfNull(otToIpp), useOriginalId);
        }
        return builder.build();
    }

    private void applyIdProcessingParameters(GtfsRealtime.FeedEntity.Builder builder,
                                             Map<ObjectType, IdProcessingParameters> otToIpp, boolean useOriginalId) {
        if (builder.hasAlert()) {
            builder.setAlert(applyIdProcessingParameters(builder.getAlertBuilder(), otToIpp, useOriginalId));
        }
        if (builder.hasTripUpdate()) {
            applyIdProcessingParameters(builder.getTripUpdateBuilder(), otToIpp, useOriginalId);
        }
        if (builder.hasVehicle()) {
            applyIdProcessingParameters(builder.getVehicleBuilder(), otToIpp, useOriginalId);
        }
    }

    private GtfsRealtime.Alert applyIdProcessingParameters(GtfsRealtime.Alert.Builder builder, Map<ObjectType,
            IdProcessingParameters> otToIpp, boolean useOriginalId) {
        builder.getInformedEntityBuilderList().forEach(e -> applyIdProcessingParameters(e, otToIpp, useOriginalId));
        return builder.build();
    }

    private void applyIdProcessingParameters(GtfsRealtime.EntitySelector.Builder builder,
                                             Map<ObjectType,
                                                     IdProcessingParameters> otToIpp, boolean useOriginalId) {
        if (builder.hasAgencyId()) {
            builder.setAgencyId(applyIdProcessingParameter(builder.getAgencyId(),
                    otToIpp.get(ObjectType.OPERATOR)));
        }
        if (builder.hasRouteId()) {
            builder.setRouteId(applyIdProcessingParameterForLine(builder.getRouteId(), otToIpp.get(ObjectType.LINE)));
        }
        if (builder.hasStopId()) {
            builder.setStopId(applyIdProcessingParameterForStop(builder.getStopId(), otToIpp.get(ObjectType.STOP),
                    useOriginalId));
        }
        if (builder.hasTrip()) {
            applyIdProcessingParameters(builder.getTripBuilder(), otToIpp);
        }
    }

    private void applyIdProcessingParameters(GtfsRealtime.TripUpdate.Builder builder, Map<ObjectType,
            IdProcessingParameters> otToIpp, boolean useOriginalId) {
        if (builder.hasTrip()) {
            applyIdProcessingParameters(builder.getTripBuilder(), otToIpp);
        }
        for (GtfsRealtime.TripUpdate.StopTimeUpdate.Builder stuBuilder : builder.getStopTimeUpdateBuilderList()) {
            if (stuBuilder.hasStopId()) {
                stuBuilder.setStopId(applyIdProcessingParameterForStop(stuBuilder.getStopId(),
                        otToIpp.get(ObjectType.STOP), useOriginalId));
            }
            var stpBuilder =
                    stuBuilder.getStopTimePropertiesBuilder();
            if (stpBuilder.hasAssignedStopId()) {
                stpBuilder.setAssignedStopId(applyIdProcessingParameterForStop(stpBuilder.getAssignedStopId(),
                        otToIpp.get(ObjectType.STOP), useOriginalId));
            }
            if (builder.hasTripProperties()) {
                var tpBuilder = builder.getTripPropertiesBuilder();
                if (tpBuilder.hasTripId()) {
                    tpBuilder.setTripId(applyIdProcessingParameter(tpBuilder.getTripId(), otToIpp.get(ObjectType.VEHICLE_JOURNEY)));
                }
            }
        }

    }

    private void applyIdProcessingParameters(GtfsRealtime.VehiclePosition.Builder builder, Map<ObjectType, IdProcessingParameters> otToIpp, boolean useOriginalId) {
        if (builder.hasTrip()) {
            applyIdProcessingParameters(builder.getTripBuilder(), otToIpp);
        }
        if (builder.hasStopId()) {
            builder.setStopId(applyIdProcessingParameterForStop(builder.getStopId(), otToIpp.get(ObjectType.STOP), useOriginalId));
        }
    }

    private void applyIdProcessingParameters(GtfsRealtime.TripDescriptor.Builder builder, Map<ObjectType,
            IdProcessingParameters> otToIpp) {
        if (builder.hasRouteId()) {
            builder.setRouteId(applyIdProcessingParameterForLine(builder.getRouteId(), otToIpp.get(ObjectType.LINE)));
        }
        if (builder.hasTripId()) {
            builder.setTripId(applyIdProcessingParameter(builder.getTripId(), otToIpp.get(ObjectType.VEHICLE_JOURNEY)));
        }
        builder.build();
    }

    private String applyIdProcessingParameter(String id, IdProcessingParameters ipp) {
        if (ipp != null) {
            return ipp.applyTransformationToString(id);
        }
        return id;
    }

    private String applyIdProcessingParameterForLine(String lineId, IdProcessingParameters ipp) {
        lineId = applyIdProcessingParameter(lineId, ipp);
        lineId = redisService.handleFlexibleLine(lineId);
        return lineId;
    }

    private String applyIdProcessingParameterForStop(String stopId, IdProcessingParameters ipp, boolean useOriginalId) {
        stopId = applyIdProcessingParameter(stopId, ipp);
        if (!useOriginalId) {
            String mobiitiId = redisService.readIdMap(RedisService.Type.ID_MAPPING, stopId);
            if (mobiitiId == null) {
                mobiitiId = redisService.readIdMap(RedisService.Type.ID_MAPPING, stopId.replace(":Quay:", ":StopPlace:"));
            }
            return mobiitiId;
        }
        return stopId;
    }

}
