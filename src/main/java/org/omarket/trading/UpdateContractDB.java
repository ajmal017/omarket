package org.omarket.trading;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.rx.java.ObservableFuture;
import io.vertx.rxjava.core.RxHelper;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.eventbus.Message;
import org.omarket.trading.verticles.MarketDataVerticle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.omarket.trading.MarketData.IBROKERS_TICKS_STORAGE_PATH;

public class UpdateContractDB {
    private final static Logger logger = LoggerFactory.getLogger(UpdateContractDB.class);

    public static void main(String[] args) throws InterruptedException {

        final Vertx vertx = Vertx.vertx();
        String[] CONTRACTS_DB_PATH = {"data", "contracts"};
        String storageDirPathName = String.join(File.separator, Arrays.asList(CONTRACTS_DB_PATH));
        Path contractsDirPath = FileSystems.getDefault().getPath(storageDirPathName);

        JsonArray defaultStoragePath = new JsonArray(Arrays.asList("data", "ticks"));
        int defaultClientId = 2;
        String defaultHost = "127.0.0.1";
        int defaultPort = 7497;
        JsonObject jsonConfig =
                new JsonObject().put(IBROKERS_TICKS_STORAGE_PATH, defaultStoragePath).put("ibrokers.clientId",
                        defaultClientId).put("ibrokers.host", defaultHost).put("ibrokers.port", defaultPort).put
                        ("runBacktestFlag", false);
        DeploymentOptions options = new DeploymentOptions().setConfig(jsonConfig);
        Observable<String> marketDataDeployment = RxHelper.deployVerticle(vertx, new MarketDataVerticle(), options);
        marketDataDeployment.flatMap(deploymentId -> {
            logger.info("succesfully deployed market data verticle: " + deploymentId);
            URL etfsResource = Thread.currentThread().getContextClassLoader().getResource("etfs.json");
            Observable<String> codesStream = Observable.empty();
            if (etfsResource != null) {
                try {
                    Path etfsPath = Paths.get(etfsResource.toURI());
                    Gson gson = new Gson();
                    JsonReader reader = new JsonReader(Files.newBufferedReader(etfsPath));
                    Type typeOfEtfsList = new TypeToken<List<String>>() {
                    }.getType();
                    List<String> ibCodesETFs = gson.fromJson(reader, typeOfEtfsList);
                    codesStream = Observable.from(ibCodesETFs);
                } catch (URISyntaxException | IOException e) {
                    logger.error("failed to load resource: ", e);
                    vertx.close();
                }
            }
            return codesStream;
        }).concatMap(address -> Observable.just(address).delay(100, TimeUnit.MILLISECONDS))  // throttling
                .flatMap(code -> {
                    JsonObject contract = new JsonObject().put("conId", code);
                    ObservableFuture<Message<JsonObject>> contractStream = io.vertx.rx.java.RxHelper.observableFuture();
                    DeliveryOptions deliveryOptions = new DeliveryOptions();
                    deliveryOptions.setSendTimeout(10000);
                    vertx.eventBus().send(MarketDataVerticle.ADDRESS_CONTRACT_RETRIEVE, contract, deliveryOptions,
                            contractStream.toHandler());
                    return contractStream;
                }).doOnNext(result -> {
            JsonObject error = result.body().getJsonObject("error");
            if (!error.equals(MarketDataVerticle.EMPTY)) {
                logger.error("error occured:" + error);
            }
        }).filter(response -> response.body().getJsonObject("error").equals(MarketDataVerticle.EMPTY)).subscribe
                (response -> {
            JsonObject envelopJson = response.body();
            JsonObject product = envelopJson.getJsonObject("content");
            try {
                ContractDB.saveContract(contractsDirPath, product);
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
