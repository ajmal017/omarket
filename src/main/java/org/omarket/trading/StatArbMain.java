package org.omarket.trading;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Verticle;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.RxHelper;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.omarket.trading.verticles.*;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import rx.Observable;

import java.util.Arrays;

import static org.omarket.trading.MarketData.IBROKERS_TICKS_STORAGE_PATH;
import static rx.Observable.merge;

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

        Verticle marketDataVerticle = new FakeMarketDataVerticle();
        Verticle historicalDataVerticle = new HistoricalDataVerticle();
        Verticle singleLegMeanReversionStrategyVerticle = new SingleLegMeanReversionStrategyVerticle();

        Observable<String> marketDataDeployment = RxHelper.deployVerticle(vertx, marketDataVerticle, options);
        Observable<String> historicalDataDeployment = RxHelper.deployVerticle(vertx, historicalDataVerticle, options);
        Observable<String> singleLegMeanReversionStrategyDeployment = RxHelper.deployVerticle(vertx, singleLegMeanReversionStrategyVerticle, options);

        Observable<String> loggedMarketDataDeployment = marketDataDeployment
                .doOnNext(marketDataId -> logger.info("market data verticle deployed as " + marketDataId))
                .doOnError(logger::error);
        Observable<String> loggedHistoricalDataDeployment = historicalDataDeployment
                .doOnNext(historicalDataId -> logger.info("historical data verticle deployed as " + historicalDataId))
                .doOnError(logger::error);
        Observable<String> loggedSingleLegMeanReversionStrategyDeployment = singleLegMeanReversionStrategyDeployment
                .doOnNext(strategyId -> {
                logger.info("strategy verticle deployed as " + strategyId); })
                .doOnError(logger::error);

        Observable<String> dataDeployment = merge(loggedMarketDataDeployment, loggedHistoricalDataDeployment);
        Observable
                .concat(dataDeployment, loggedSingleLegMeanReversionStrategyDeployment)
                .last()
                .doOnError(onError -> logger.error("failed deploying verticles", onError))
                .doOnNext(onNext -> logger.info("all verticles deployed"))
                .subscribe();
    }
}
