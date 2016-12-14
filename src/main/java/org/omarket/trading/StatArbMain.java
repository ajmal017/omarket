package org.omarket.trading;

import io.vertx.core.DeploymentOptions;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.RxHelper;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.omarket.trading.verticles.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.util.Arrays;

import static org.omarket.trading.MarketData.IBROKERS_TICKS_STORAGE_PATH;

public class StatArbMain {
    private final static Logger logger = LoggerFactory.getLogger(StatArbMain.class);

    public static void main(String[] args) throws InterruptedException {

        JsonArray defaultStoragePath = new JsonArray(Arrays.asList("data", "ticks"));
        int defaultClientId = 1;
        String defaultHost = "127.0.0.1";
        int defaultPort = 7497;
        JsonObject jsonConfig = new JsonObject()
                .put(IBROKERS_TICKS_STORAGE_PATH, defaultStoragePath)
                .put("ibrokers.clientId", defaultClientId)
                .put("ibrokers.host", defaultHost)
                .put("ibrokers.port", defaultPort)
                .put("runBacktestFlag", false);
        DeploymentOptions options = new DeploymentOptions().setConfig(jsonConfig);

        final Vertx vertx = Vertx.vertx();

        FakeMarketDataVerticle marketDataVerticle = new FakeMarketDataVerticle();
        Observable<String> marketDataDeployment = RxHelper.deployVerticle(vertx, marketDataVerticle, options);

        marketDataDeployment
                .take(1)
                .map(marketDataId -> {
                    logger.info("market data verticle deployed as " + marketDataId);
                    return RxHelper.deployVerticle(vertx, new SingleLegMeanReversionStrategyVerticle(), options);
                })
                .flatMap(strategyId -> strategyId)
                .doOnNext(strategyId -> {
                    logger.info("strategy verticle deployed as " + strategyId);
                })
                .subscribe(
                        onNext -> {
                            logger.info("all verticles deployed");
                        }, onError -> {
                            logger.error("failed deploying verticles", onError);
                        });
    }
}
