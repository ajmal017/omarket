package org.omarket.trading;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.rx.java.ObservableFuture;
import io.vertx.rxjava.core.RxHelper;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.eventbus.Message;
import org.omarket.trading.verticles.MarketDataVerticle;
import org.omarket.trading.verticles.VerticleProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

public class RecorderMain {
    private final static Logger logger = LoggerFactory.getLogger(RecorderMain.class);

    public static void main(String[] args) throws InterruptedException {
        int defaultClientId = 3;
        DeploymentOptions options = VerticleProperties.makeDeploymentOptions(defaultClientId);

        final Vertx vertx = Vertx.vertx();

        Observable<String> marketDataDeployment = RxHelper.deployVerticle(vertx, new MarketDataVerticle(), options);
        String[] ibCodes = new String[]{"12087817", "12087820", "37893488", "28027110", "188989072", "229726316"};
        marketDataDeployment.subscribe(deploymentId -> {
            logger.info("succesfully deployed market data verticle: " + deploymentId);
            for(String ibCode: ibCodes){
                logger.info("subscribing ibCode: " + ibCode);
                JsonObject contract = new JsonObject().put("conId", ibCode);
                ObservableFuture<Message<JsonObject>> contractStream = io.vertx.rx.java.RxHelper.observableFuture();
                vertx.eventBus().send(MarketDataVerticle.ADDRESS_CONTRACT_RETRIEVE, contract, contractStream.toHandler());
                contractStream.subscribe((Message<JsonObject> contractMessage) -> {
                    JsonObject envelopJson = contractMessage.body();
                    if(!envelopJson.getJsonObject("error").isEmpty()){
                        return;
                    }
                    JsonObject contractJson = envelopJson.getJsonObject("content");
                    logger.info("contract retrieved: " + contractJson);
                    ObservableFuture<Message<JsonObject>> quoteStream = io.vertx.rx.java.RxHelper.observableFuture();
                    vertx.eventBus().send(MarketDataVerticle.ADDRESS_SUBSCRIBE_TICK, contractJson, quoteStream.toHandler());
                    quoteStream.subscribe(resultMessage -> {
                        JsonObject result = resultMessage.body();
                        logger.info("received: " + result);
                    }, err -> {
                        logger.error("error", err);
                    });
                });
            }
        });
    }
}
