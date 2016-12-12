package org.omarket.trading;

import com.ib.client.Contract;
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
import static rx.Observable.combineLatest;

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
        Observable<Integer> ibCodes = Observable.from(new Integer[]{12087817, 12087820, 37893488, 28027110});

        Observable<String> deployedMarketData = marketDataDeployment.first(deploymentId -> {
            logger.info("succesfully deployed market data verticle: " + deploymentId);
            return true;
        });
        combineLatest(deployedMarketData, ibCodes, (deploymentId, ibCode) -> ibCode)
                .flatMap(ibCode -> {
                    logger.info("subscribing ibCode: " + ibCode);
                    JsonObject contract = new JsonObject().put("conId", Integer.toString(ibCode));
                    ObservableFuture<Message<JsonObject>> contractStream = io.vertx.rx.java.RxHelper.observableFuture();
                    vertx.eventBus().send(MarketDataVerticle.ADDRESS_CONTRACT_RETRIEVE, contract, contractStream.toHandler());
                    return contractStream;
                })
                .flatMap((Message<JsonObject> contractMessage) -> {
                    JsonObject contract = contractMessage.body();
                    logger.info("contract retrieved: " + contract);
                    ObservableFuture<Message<JsonObject>> quoteStream = io.vertx.rx.java.RxHelper.observableFuture();
                    vertx.eventBus().send(MarketDataVerticle.ADDRESS_SUBSCRIBE_TICK, contract, quoteStream.toHandler());
                    return quoteStream;
                })
                .subscribe(resultMessage -> {
                    JsonObject result = resultMessage.body();
                    logger.info("received: " + result);
                }, err -> {
                    logger.error("error", err);
                });
        /*combineLatest(marketDataDeployment.take(1), (x,y)->{ logger.info("combined: " + x);})
                .subscribe(onNext -> {
                    logger.info("all verticles deployed:" + onNext);

                }, onError -> {
                    logger.error("failed deploying verticles", onError);
                    vertx.close();
                });*/
        /*
            JsonObject product = new JsonObject().put("conId", Integer.toString(ibCode));
            ObservableFuture<Message<JsonObject>> contractStream = io.vertx.rx.java.RxHelper.observableFuture();
            logger.info("requesting subscription for product: " + ibCode);
            vertx.eventBus().send(MarketDataVerticle.ADDRESS_CONTRACT_RETRIEVE, product, contractStream.toHandler());
            return contractStream;
        })*/

    }
}
