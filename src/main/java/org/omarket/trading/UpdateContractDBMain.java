package org.omarket.trading;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.eventbus.DeliveryOptions;
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
import java.lang.reflect.Type;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class UpdateContractDBMain {
    private final static Logger logger = LoggerFactory.getLogger(UpdateContractDBMain.class);

    public static void main(String[] args) throws InterruptedException {

        final Vertx vertx = Vertx.vertx();
        int defaultClientId = 2;
        DeploymentOptions options = VerticleProperties.makeDeploymentOptions(defaultClientId);
        Observable<String> marketDataDeployment = RxHelper.deployVerticle(vertx, new MarketDataVerticle(), options);
        marketDataDeployment.flatMap(deploymentId -> {
            logger.info("succesfully deployed market data verticle: " + deploymentId);
            Observable<String> codesStream = Observable.empty();
            try {
                URL etfsResource = Thread.currentThread().getContextClassLoader().getResource("etfs.json");
                if (etfsResource != null) {
                        URI resourceURI = etfsResource.toURI();
                        Path etfsPath = Paths.get(resourceURI);
                        JsonReader reader = new JsonReader(Files.newBufferedReader(etfsPath));
                        Type typeOfEtfsList = new TypeToken<List<String>>() {
                        }.getType();
                        Gson gson = new Gson();
                        List<String> ibCodesETFs = gson.fromJson(reader, typeOfEtfsList);
                        codesStream = Observable.from(ibCodesETFs);
                }
            } catch (URISyntaxException | IOException e) {
                logger.error("failed to load resource: ", e);
                vertx.close();
            }
            return codesStream;
        }).concatMap(code -> Observable.just(code).delay(100, TimeUnit.MILLISECONDS))  // throttling
                .flatMap(new ContractFetcher(vertx)).doOnNext(result -> {
            JsonObject error = result.body().getJsonObject("error");
            if (!error.equals(MarketDataVerticle.EMPTY)) {
                logger.error("error occured:" + error);
            }
        }).filter(response -> response.body().getJsonObject("error").equals(MarketDataVerticle.EMPTY)).subscribe
                (response -> {
            JsonObject envelopJson = response.body();
            JsonObject product = envelopJson.getJsonObject("content");
            try {
                // TODO: parameters
                ContractDB.saveContract(Paths.get("data", "contracts"), product);
            } catch (IOException e) {
                logger.error("failed to save to contracts db", e);
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
