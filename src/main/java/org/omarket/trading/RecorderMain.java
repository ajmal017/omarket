package org.omarket.trading;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.rx.java.ObservableFuture;
import io.vertx.rxjava.core.RxHelper;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.eventbus.Message;
import org.omarket.trading.verticles.FakeMarketDataVerticle;
import org.omarket.trading.verticles.MarketDataVerticle;
import org.omarket.trading.verticles.SingleLegMeanReversionStrategyVerticle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.util.Arrays;

import static org.omarket.trading.verticles.MarketDataVerticle.IBROKERS_TICKS_STORAGE_PATH;

public class RecorderMain {
    private final static Logger logger = LoggerFactory.getLogger(RecorderMain.class);

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

        Observable<String> marketDataDeployment = RxHelper.deployVerticle(vertx, new MarketDataVerticle(), options);

        marketDataDeployment
                .take(1)
                .subscribe(onNext -> {
                    logger.info("all verticles deployed");
                    Observable.from(new Integer[]{12087817, 12087820, 37893488, 28027110})
                            .flatMap(ibCode -> {
                                JsonObject product = new JsonObject().put("conId", Integer.toString(ibCode));
                                ObservableFuture<Message<JsonObject>> contractStream = io.vertx.rx.java.RxHelper.observableFuture();
                                logger.info("requesting subscription for product: " + ibCode);
                                vertx.eventBus().send(MarketDataVerticle.ADDRESS_CONTRACT_RETRIEVE, product, contractStream.toHandler());
                                return contractStream;
                            }).subscribe();
                }, onError -> {
                    logger.error("failed deploying verticles", onError);
                    vertx.close();
                });

    }
}
