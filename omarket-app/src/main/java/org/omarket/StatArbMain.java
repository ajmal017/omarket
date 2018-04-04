package org.omarket;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Verticle;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.RxHelper;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import lombok.extern.slf4j.Slf4j;
import org.omarket.trading.verticles.DummyMeanReversionStrategyVerticle;
import org.omarket.trading.verticles.HistoricalDataVerticle;
import org.omarket.trading.verticles.VerticleProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.Objects;

@Slf4j
@Component
class StatArbService {
    private final VerticleProperties props;
    @Autowired
    Verticle historicalDataVerticle;
    @Autowired
    public StatArbService(VerticleProperties props) {
        this.props = props;
    }

    public void run(DeploymentOptions options) throws InterruptedException {
        final Vertx vertx = Vertx.vertx();

        Verticle singleLegMeanReversionStrategyVerticle = new DummyMeanReversionStrategyVerticle();

        RxHelper.deployVerticle(vertx, historicalDataVerticle, options)
                .subscribe(historicalDataId -> {
                    log.info("historical data verticle deployed as " + historicalDataId);
                    RxHelper.deployVerticle(vertx, singleLegMeanReversionStrategyVerticle, options)
                            .doOnNext(strategyId -> {
                                log.info("strategy verticle deployed as " + strategyId);
                            })
                            .doOnError(err -> log.error("failed to deploy", err));
                })
                ;
    }

}

@Slf4j
@Component
class StatArbRunner implements ApplicationRunner {

    private final StatArbService service;

    private final VerticleProperties props;

    @Value("${org.omarket.client_id.statarb}")
    private String clientId;

    @Autowired
    public StatArbRunner(StatArbService service, VerticleProperties props) {
        this.service = service;
        this.props = props;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        if (Objects.equals(args.getNonOptionArgs().get(0), "statarb")) {
            DeploymentOptions options = props.makeDeploymentOptions(Integer.valueOf(clientId));
            service.run(options);
        }
    }
}
