package org.omarket.trading;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.core.RxHelper;
import io.vertx.rxjava.core.Vertx;
import org.omarket.trading.verticles.MarketDataVerticle;
import org.omarket.trading.verticles.VerticleProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

public class UpdateEODMain {
    private final static Logger logger = LoggerFactory.getLogger(UpdateEODMain.class);

    public static void main(String[] args) throws InterruptedException {

        final Vertx vertx = Vertx.vertx();
        int defaultClientId = 4;
        DeploymentOptions options = VerticleProperties.makeDeploymentOptions(defaultClientId);
        Observable<String> marketDataDeployment = RxHelper.deployVerticle(vertx, new MarketDataVerticle(), options);
        marketDataDeployment.subscribe(deploymentId -> {
            logger.info("succesfully deployed market data verticle: " + deploymentId);
            ContractDB.ContractFilter filter = new ContractDB.ContractFilter() {
                @Override
                public boolean accept(Path path) {
                    return false;
                }
            };
            JsonArray contractsStream = null;
            try {
                contractsStream = ContractDB.loadContracts(Paths.get("data", "contracts"), filter);
            } catch (IOException e) {
                logger.error("an error occured while accessing contracts DB", e);
            }
        }, failed -> {
            logger.error("terminating - unrecoverable error occurred:" + failed);
            System.exit(0);
        }, () -> {
            logger.info("completed");
            System.exit(0);
        });
        /*
        contractsStream.flatMap(contractMessage -> {
            JsonObject envelopJson = contractMessage.body();
            JsonObject product = envelopJson.getJsonObject("content");
                    JsonObject contract = product.getJsonObject("m_contract");
                    logger.info("processing contract: " + contract);
            DeliveryOptions deliveryOptions = new DeliveryOptions();
            deliveryOptions.setSendTimeout(10000);
            ObservableFuture<Message<JsonArray>> eodStream = io.vertx.rx.java.RxHelper.observableFuture();
            vertx.eventBus().send(MarketDataVerticle.ADDRESS_EOD_REQUEST, product, deliveryOptions, eodStream
            .toHandler());
            return eodStream;
        }).subscribe(barsMessage -> {
            JsonArray bars = barsMessage.body();
            logger.info("next: " + bars);
        }, failed -> {
            logger.error("terminating - unrecoverable error occurred:" + failed);
            System.exit(0);
        }, () -> {
            logger.info("completed");
            System.exit(0);
        });
        */
    }

}
