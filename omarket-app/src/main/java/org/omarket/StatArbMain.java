package org.omarket;

import io.vertx.core.Verticle;
import io.vertx.rxjava.core.RxHelper;
import io.vertx.rxjava.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import org.omarket.trading.verticles.DummyMeanReversionStrategyVerticle;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.Objects;

@Slf4j
@Component
class StatArbService {
    @Autowired
    Verticle historicalDataVerticle;

    public void run() throws InterruptedException {
        final Vertx vertx = Vertx.vertx();

        Verticle singleLegMeanReversionStrategyVerticle = new DummyMeanReversionStrategyVerticle();

        RxHelper.deployVerticle(vertx, historicalDataVerticle)
                .subscribe(historicalDataId -> {
                    log.info("historical data verticle deployed as " + historicalDataId);
                    RxHelper.deployVerticle(vertx, singleLegMeanReversionStrategyVerticle)
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

    @Value("${org.omarket.client_id.statarb}")
    private String clientId;

    @Autowired
    public StatArbRunner(StatArbService service) {
        this.service = service;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        if (Objects.equals(args.getNonOptionArgs().get(0), "statarb")) {
            service.run();
        }
    }
}
