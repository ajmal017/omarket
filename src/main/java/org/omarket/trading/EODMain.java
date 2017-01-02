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

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.omarket.trading.MarketData.IBROKERS_TICKS_STORAGE_PATH;

public class EODMain {
    private final static Logger logger = LoggerFactory.getLogger(EODMain.class);

    public static void main(String[] args) throws InterruptedException {
        JsonArray defaultStoragePath = new JsonArray(Arrays.asList("data", "ticks"));
        int defaultClientId = 1;
        String defaultHost = "127.0.0.1";
        int defaultPort = 7497;
        JsonObject jsonConfig =
                new JsonObject().put(IBROKERS_TICKS_STORAGE_PATH, defaultStoragePath).put("ibrokers.clientId",
                        defaultClientId).put("ibrokers.host", defaultHost).put("ibrokers.port", defaultPort).put
                        ("runBacktestFlag", false);
        DeploymentOptions options = new DeploymentOptions().setConfig(jsonConfig);

        final Vertx vertx = Vertx.vertx();

        Observable<String> marketDataDeployment = RxHelper.deployVerticle(vertx, new MarketDataVerticle(), options);
        marketDataDeployment.subscribe(deploymentId -> {
            logger.info("succesfully deployed market data verticle: " + deploymentId);
            URL etfsResource = Thread.currentThread().getContextClassLoader().getResource("etfs.json");
            if (etfsResource == null) {
                vertx.close();
                return;
            }
            try {
                Path etfsPath = Paths.get(etfsResource.toURI());
                Gson gson = new Gson();
                JsonReader reader = new JsonReader(Files.newBufferedReader(etfsPath));
                Type typeOfEtfsList = new TypeToken<List<String>>() {
                }.getType();
                List<String> ibCodesETFs = gson.fromJson(reader, typeOfEtfsList);
                Stream<String> codesStream = ibCodesETFs.stream().skip(50).limit(4);
                for (String ibCode : codesStream.collect(Collectors.toList())) {
                    JsonObject contract = new JsonObject().put("conId", ibCode);
                    ObservableFuture<Message<JsonObject>> contractStream = io.vertx.rx.java.RxHelper.observableFuture();
                    DeliveryOptions deliveryOptions = new DeliveryOptions();
                    deliveryOptions.setSendTimeout(10000);
                    vertx.eventBus().send(MarketDataVerticle.ADDRESS_CONTRACT_RETRIEVE, contract, deliveryOptions, contractStream
                            .toHandler());
                    contractStream.subscribe((Message<JsonObject> contractMessage) -> {
                        JsonObject contractDetails = contractMessage.body();
                        logger.info("received data for contract: " + contractDetails);
                    }, error -> {
                        logger.error("unrecoverable error occured", error);
                    });
                }
            } catch (URISyntaxException | IOException e) {
                logger.error("failed to load resource: ", e);
                vertx.close();
            }
        }, err -> {
            logger.error("shutting down vertx", err);
            vertx.close();
        });
    }
}
