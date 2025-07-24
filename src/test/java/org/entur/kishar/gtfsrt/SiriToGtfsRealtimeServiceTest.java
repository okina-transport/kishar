package org.entur.kishar.gtfsrt;

import org.entur.kishar.App;
import org.entur.kishar.config.SubscriptionConfig;
import org.entur.kishar.gtfsrt.mappers.IdMapper;
import org.entur.kishar.metrics.PrometheusMetricsService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.junit.jupiter.SpringExtension;


@ExtendWith(SpringExtension.class)
@Configuration
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK, classes = App.class)
public abstract class SiriToGtfsRealtimeServiceTest {

    protected SiriToGtfsRealtimeService rtService;
    protected IdMapper idMapper;
    @Mock
    protected RedisService redisService;
    @Mock
    protected PrometheusMetricsService prometheusMetricsService;
    @Mock
    protected SubscriptionConfig subscriptionConfig;
    @Mock
    protected GtfsTripsService gtfsTripsService;
    @Value("${kishar.settings.vm.close.to.stop.percentage}")
    int NEXT_STOP_PERCENTAGE;
    @Value("${kishar.settings.vm.close.to.stop.distance}")
    int NEXT_STOP_DISTANCE;
    @Value("${kishar.settings.vm.close.to.stop.percentage}")
    int closeToNextStopPercentage;
    @Value("${kishar.settings.vm.close.to.stop.distance}")
    int closeToNextStopDistance;

    @BeforeEach
    void before() {
        idMapper = new IdMapper(subscriptionConfig, redisService);
        rtService = new SiriToGtfsRealtimeService(new AlertFactory(),
                redisService,
                prometheusMetricsService,
                idMapper,
                closeToNextStopPercentage,
                closeToNextStopDistance,
                gtfsTripsService);
    }

    @AfterEach
    void cleanup() {
        //Deletes all received data
        rtService.clearGtfsRtCache();
    }
}
