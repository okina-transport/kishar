package org.entur.kishar.gtfsrt.mappers;


import com.google.transit.realtime.GtfsRealtime;
import org.entur.kishar.config.SubscriptionConfig;
import org.entur.kishar.gtfsrt.RedisService;
import org.entur.kishar.utils.IdProcessingParameters;
import org.entur.kishar.utils.ObjectType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class IdMapperTest {

    public static final String LINE_ID = "LINE_ID";
    public static final String VJ_ID = "VJ_ID";
    public static final String OPERATOR_ID = "OPERATOR_ID";
    public static final String EXPECTED_MOBIITI_STOP_ID = "MOBIITI:Quay:1:LOC";
    private static final String STOP_ID = "STOP_ID";
    private static final String DATASET = "TST";
    private static final String OUTPUT_PREFIX_TO_ADD_STOP = "NAOLIBORG:StopPlace:";
    private static final String OUTPUT_PREFIX_TO_ADD_LINE = "NAOLIBORG:Line:";
    private static final String OUTPUT_PREFIX_TO_ADD_VJ = "NAOLIBORG:VehicleJourney:";
    private static final String OUTPUT_PREFIX_TO_ADD_OPERATOR = "NAOLIBORG:Operator:";
    private static final String OUTPUT_SUFFIX_TO_ADD = ":LOC";
    public static final String EXPECTED_STOP_ID = OUTPUT_PREFIX_TO_ADD_STOP + STOP_ID + OUTPUT_SUFFIX_TO_ADD;
    public static final String EXPECTED_LINE_ID = OUTPUT_PREFIX_TO_ADD_LINE + LINE_ID + OUTPUT_SUFFIX_TO_ADD;
    public static final String EXPECTED_VJ_ID = OUTPUT_PREFIX_TO_ADD_VJ + VJ_ID + OUTPUT_SUFFIX_TO_ADD;
    public static final String EXPECTED_OPERATOR_ID = OUTPUT_PREFIX_TO_ADD_OPERATOR + OPERATOR_ID + OUTPUT_SUFFIX_TO_ADD;
    @InjectMocks
    private IdMapper idMapper;
    @Mock
    private SubscriptionConfig subscriptionConfig;
    @Mock
    private RedisService redisService;

    @Test
    void test_applyIdProcessingParameters_whenThereIsNoIppForDataset_thenDoNothing() {
        // Arrange
        when(redisService.handleFlexibleLine(any())).then(invocation -> invocation.getArguments()[0]);
        when(subscriptionConfig.getIdParametersForDataset(DATASET)).thenReturn(null);
        GtfsRealtime.FeedMessage message =
                GtfsRealtime.FeedMessage.newBuilder()
                        .setHeader(
                                GtfsRealtime.FeedHeader.newBuilder()
                                        .setGtfsRealtimeVersion("1.0")
                        )
                        .addEntity(GtfsRealtime.FeedEntity.newBuilder()
                                .setId("ID1")
                                .setAlert(GtfsRealtime.Alert.newBuilder().addInformedEntity(
                                        GtfsRealtime.EntitySelector.newBuilder()
                                                .setAgencyId(OPERATOR_ID)
                                                .setRouteId(LINE_ID)
                                                .setStopId(STOP_ID)
                                                .setTrip(
                                                        GtfsRealtime.TripDescriptor.newBuilder()
                                                                .setTripId(VJ_ID)
                                                                .setRouteId(LINE_ID)
                                                ))
                                ))
                        .addEntity(GtfsRealtime.FeedEntity.newBuilder()
                                .setId("ID2")
                                .setTripUpdate(GtfsRealtime.TripUpdate.newBuilder()
                                        .setTrip(
                                                GtfsRealtime.TripDescriptor.newBuilder()
                                                        .setTripId(VJ_ID)
                                                        .setRouteId(LINE_ID))
                                        .addStopTimeUpdate(GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder()
                                                .setStopId(STOP_ID)
                                                .setStopTimeProperties(
                                                        GtfsRealtime.TripUpdate.StopTimeUpdate.StopTimeProperties.newBuilder()
                                                                .setAssignedStopId(STOP_ID)
                                                ))
                                ))
                        .addEntity(GtfsRealtime.FeedEntity.newBuilder()
                                .setId("ID3")
                                .setVehicle(GtfsRealtime.VehiclePosition.newBuilder()
                                        .setTrip(
                                                GtfsRealtime.TripDescriptor.newBuilder()
                                                        .setTripId(VJ_ID)
                                                        .setRouteId(LINE_ID))
                                        .setStopId(STOP_ID)
                                ))
                        .build();

        // Act
        GtfsRealtime.FeedMessage output = idMapper.applyIdProcessingParameters(message, DATASET, true);

        // Assert
        assertThat(output).isEqualTo(message);
    }

    @Test
    void test_applyIdProcessingParameters_whenThereIsIppForDataset_thenApplyIppCorrectly() {
        // Arrange
        IdProcessingParameters ippStop = new IdProcessingParameters();
        ippStop.setOutputPrefixToAdd(OUTPUT_PREFIX_TO_ADD_STOP);
        ippStop.setOutputSuffixToAdd(OUTPUT_SUFFIX_TO_ADD);
        IdProcessingParameters ippLine = new IdProcessingParameters();
        ippLine.setOutputPrefixToAdd(OUTPUT_PREFIX_TO_ADD_LINE);
        ippLine.setOutputSuffixToAdd(OUTPUT_SUFFIX_TO_ADD);
        IdProcessingParameters ippVj = new IdProcessingParameters();
        ippVj.setOutputPrefixToAdd(OUTPUT_PREFIX_TO_ADD_VJ);
        ippVj.setOutputSuffixToAdd(OUTPUT_SUFFIX_TO_ADD);
        IdProcessingParameters ippOperator = new IdProcessingParameters();
        ippOperator.setOutputPrefixToAdd(OUTPUT_PREFIX_TO_ADD_OPERATOR);
        ippOperator.setOutputSuffixToAdd(OUTPUT_SUFFIX_TO_ADD);

        when(redisService.handleFlexibleLine(any())).then(invocation -> invocation.getArguments()[0]);
        when(subscriptionConfig.getIdParametersForDataset(DATASET)).thenReturn(
                Map.of(ObjectType.STOP, ippStop,
                        ObjectType.LINE, ippLine,
                        ObjectType.VEHICLE_JOURNEY, ippVj,
                        ObjectType.OPERATOR, ippOperator
                )
        );

        GtfsRealtime.FeedMessage message =
                GtfsRealtime.FeedMessage.newBuilder()
                        .setHeader(
                                GtfsRealtime.FeedHeader.newBuilder()
                                        .setGtfsRealtimeVersion("1.0")
                        )
                        .addEntity(GtfsRealtime.FeedEntity.newBuilder()
                                .setId("ID1")
                                .setAlert(GtfsRealtime.Alert.newBuilder().addInformedEntity(
                                        GtfsRealtime.EntitySelector.newBuilder()
                                                .setAgencyId(OPERATOR_ID)
                                                .setRouteId(LINE_ID)
                                                .setStopId(STOP_ID)
                                                .setTrip(
                                                        GtfsRealtime.TripDescriptor.newBuilder()
                                                                .setTripId(VJ_ID)
                                                                .setRouteId(LINE_ID)
                                                ))
                                ))
                        .addEntity(GtfsRealtime.FeedEntity.newBuilder()
                                .setId("ID2")
                                .setTripUpdate(GtfsRealtime.TripUpdate.newBuilder()
                                        .setTrip(
                                                GtfsRealtime.TripDescriptor.newBuilder()
                                                        .setTripId(VJ_ID)
                                                        .setRouteId(LINE_ID))
                                        .addStopTimeUpdate(GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder()
                                                .setStopId(STOP_ID)
                                                .setStopTimeProperties(
                                                        GtfsRealtime.TripUpdate.StopTimeUpdate.StopTimeProperties.newBuilder()
                                                                .setAssignedStopId(STOP_ID)
                                                ))
                                ))
                        .addEntity(GtfsRealtime.FeedEntity.newBuilder()
                                .setId("ID3")
                                .setVehicle(GtfsRealtime.VehiclePosition.newBuilder()
                                        .setTrip(
                                                GtfsRealtime.TripDescriptor.newBuilder()
                                                        .setTripId(VJ_ID)
                                                        .setRouteId(LINE_ID))
                                        .setStopId(STOP_ID)
                                ))
                        .build();

        // Act
        GtfsRealtime.FeedMessage output = idMapper.applyIdProcessingParameters(message, DATASET, true);

        // Assert
        // Alert
        GtfsRealtime.EntitySelector entity = output.getEntity(0).getAlert().getInformedEntity(0);
        assertThat(entity.getAgencyId()).isEqualTo(EXPECTED_OPERATOR_ID);
        assertThat(entity.getRouteId()).isEqualTo(EXPECTED_LINE_ID);
        assertThat(entity.getStopId()).isEqualTo(EXPECTED_STOP_ID);
        assertThat(entity.getTrip().getRouteId()).isEqualTo(EXPECTED_LINE_ID);
        assertThat(entity.getTrip().getTripId()).isEqualTo(EXPECTED_VJ_ID);

        // TripUpdate
        GtfsRealtime.TripUpdate tripUpdate = output.getEntity(1).getTripUpdate();
        assertThat(tripUpdate.getTrip().getRouteId()).isEqualTo(EXPECTED_LINE_ID);
        assertThat(tripUpdate.getTrip().getTripId()).isEqualTo(EXPECTED_VJ_ID);
        assertThat(tripUpdate.getStopTimeUpdate(0).getStopId()).isEqualTo(EXPECTED_STOP_ID);
        assertThat(tripUpdate.getStopTimeUpdate(0).getStopTimeProperties().getAssignedStopId()).isEqualTo(EXPECTED_STOP_ID);

        // VehiclePosition
        GtfsRealtime.VehiclePosition vehicle = output.getEntity(2).getVehicle();
        assertThat(vehicle.getTrip().getRouteId()).isEqualTo(EXPECTED_LINE_ID);
        assertThat(vehicle.getTrip().getTripId()).isEqualTo(EXPECTED_VJ_ID);
        assertThat(vehicle.getStopId()).isEqualTo(EXPECTED_STOP_ID);
    }

    @Test
    void test_applyIdProcessingParameters_whenUseOriginalIdIsFalse_thenReturnStopMobiitiIdFromCache() {
        // Arrange
        IdProcessingParameters ippStop = new IdProcessingParameters();
        ippStop.setOutputPrefixToAdd(OUTPUT_PREFIX_TO_ADD_STOP);
        ippStop.setOutputSuffixToAdd(OUTPUT_SUFFIX_TO_ADD);
        IdProcessingParameters ippLine = new IdProcessingParameters();
        ippLine.setOutputPrefixToAdd(OUTPUT_PREFIX_TO_ADD_LINE);
        ippLine.setOutputSuffixToAdd(OUTPUT_SUFFIX_TO_ADD);
        IdProcessingParameters ippVj = new IdProcessingParameters();
        ippVj.setOutputPrefixToAdd(OUTPUT_PREFIX_TO_ADD_VJ);
        ippVj.setOutputSuffixToAdd(OUTPUT_SUFFIX_TO_ADD);
        IdProcessingParameters ippOperator = new IdProcessingParameters();
        ippOperator.setOutputPrefixToAdd(OUTPUT_PREFIX_TO_ADD_OPERATOR);
        ippOperator.setOutputSuffixToAdd(OUTPUT_SUFFIX_TO_ADD);

        when(redisService.handleFlexibleLine(any())).then(invocation -> invocation.getArguments()[0]);
        when(subscriptionConfig.getIdParametersForDataset(DATASET)).thenReturn(
                Map.of(ObjectType.STOP, ippStop,
                        ObjectType.LINE, ippLine,
                        ObjectType.VEHICLE_JOURNEY, ippVj,
                        ObjectType.OPERATOR, ippOperator
                )
        );
        when(redisService.readIdMap(RedisService.Type.ID_MAPPING, EXPECTED_STOP_ID)).thenReturn(EXPECTED_MOBIITI_STOP_ID);

        GtfsRealtime.FeedMessage message =
                GtfsRealtime.FeedMessage.newBuilder()
                        .setHeader(
                                GtfsRealtime.FeedHeader.newBuilder()
                                        .setGtfsRealtimeVersion("1.0")
                        )
                        .addEntity(GtfsRealtime.FeedEntity.newBuilder()
                                .setId("ID1")
                                .setAlert(GtfsRealtime.Alert.newBuilder().addInformedEntity(
                                        GtfsRealtime.EntitySelector.newBuilder()
                                                .setAgencyId(OPERATOR_ID)
                                                .setRouteId(LINE_ID)
                                                .setStopId(STOP_ID)
                                                .setTrip(
                                                        GtfsRealtime.TripDescriptor.newBuilder()
                                                                .setTripId(VJ_ID)
                                                                .setRouteId(LINE_ID)
                                                ))
                                ))
                        .addEntity(GtfsRealtime.FeedEntity.newBuilder()
                                .setId("ID2")
                                .setTripUpdate(GtfsRealtime.TripUpdate.newBuilder()
                                        .setTrip(
                                                GtfsRealtime.TripDescriptor.newBuilder()
                                                        .setTripId(VJ_ID)
                                                        .setRouteId(LINE_ID))
                                        .addStopTimeUpdate(GtfsRealtime.TripUpdate.StopTimeUpdate.newBuilder()
                                                .setStopId(STOP_ID)
                                                .setStopTimeProperties(
                                                        GtfsRealtime.TripUpdate.StopTimeUpdate.StopTimeProperties.newBuilder()
                                                                .setAssignedStopId(STOP_ID)
                                                ))
                                ))
                        .addEntity(GtfsRealtime.FeedEntity.newBuilder()
                                .setId("ID3")
                                .setVehicle(GtfsRealtime.VehiclePosition.newBuilder()
                                        .setTrip(
                                                GtfsRealtime.TripDescriptor.newBuilder()
                                                        .setTripId(VJ_ID)
                                                        .setRouteId(LINE_ID))
                                        .setStopId(STOP_ID)
                                ))
                        .build();

        // Act
        GtfsRealtime.FeedMessage output = idMapper.applyIdProcessingParameters(message, DATASET, false);

        // Assert
        // Alert
        GtfsRealtime.EntitySelector entity = output.getEntity(0).getAlert().getInformedEntity(0);
        assertThat(entity.getStopId()).isEqualTo(EXPECTED_MOBIITI_STOP_ID);

        // TripUpdate
        GtfsRealtime.TripUpdate tripUpdate = output.getEntity(1).getTripUpdate();
        assertThat(tripUpdate.getStopTimeUpdate(0).getStopId()).isEqualTo(EXPECTED_MOBIITI_STOP_ID);
        assertThat(tripUpdate.getStopTimeUpdate(0).getStopTimeProperties().getAssignedStopId()).isEqualTo(EXPECTED_MOBIITI_STOP_ID);

        // VehiclePosition
        GtfsRealtime.VehiclePosition vehicle = output.getEntity(2).getVehicle();
        assertThat(vehicle.getStopId()).isEqualTo(EXPECTED_MOBIITI_STOP_ID);
    }


}