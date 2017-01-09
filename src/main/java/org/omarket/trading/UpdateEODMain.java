package org.omarket.trading;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonArray;
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

import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

public class UpdateEODMain {
    private final static Logger logger = LoggerFactory.getLogger(UpdateEODMain.class);

    public static void main(String[] args) throws InterruptedException, IOException {
        ContractDB.ContractFilter filter = new ContractDB.ContractFilter() {
            @Override
            public boolean accept(String content) {
                boolean exchangeMatch = getPrimaryExchange().equals("ARCA");
                boolean typeMatch = getSecurityType().equals("STK");
                boolean currencyMatch = getCurrency().equals("USD");
                return exchangeMatch && typeMatch && currencyMatch;
            }
        };
        Observable<JsonObject> contracts = ContractDB.loadContracts(Paths.get("data", "contracts"), filter);
        contracts.subscribe(contract -> {
            logger.info("loaded contracts: " + contract);
        });
    }

    public static void main2(String[] args) throws InterruptedException {

        final Vertx vertx = Vertx.vertx();
        int defaultClientId = 4;
        DeploymentOptions options = VerticleProperties.makeDeploymentOptions(defaultClientId);
        Observable<String> marketDataDeployment = RxHelper.deployVerticle(vertx, new MarketDataVerticle(), options);
        marketDataDeployment.flatMap(deploymentId -> {
            logger.info("succesfully deployed market data verticle: " + deploymentId);
            ContractDB.ContractFilter filter = new ContractDB.ContractFilter() {
                @Override
                public boolean accept(String content) {
                    return getPrimaryExchange().equals("ARCA") && getSecurityType().equals("STK") && getCurrency()
                            .equals("USD");
                }
            };
            Observable<JsonObject> contracts = Observable.empty();
            try {
                contracts = ContractDB.loadContracts(Paths.get("data", "contracts"), filter);
            } catch (IOException e) {
                logger.error("an error occured while accessing contracts DB", e);
            }
            return contracts;
        }).concatMap(object -> Observable.just(object).delay(100, TimeUnit.MILLISECONDS))  // throttling
                .flatMap(object -> {
                    JsonObject product = (JsonObject) object;
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
    }

}
