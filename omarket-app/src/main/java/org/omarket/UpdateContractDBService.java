package org.omarket;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import io.vertx.core.VertxOptions;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.rx.java.ObservableFuture;
import io.vertx.rxjava.core.RxHelper;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.eventbus.Message;
import lombok.extern.slf4j.Slf4j;
import org.omarket.trading.verticles.MarketDataVerticle;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import rx.Observable;
import rx.functions.Func1;

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

@Slf4j
@Service
public class UpdateContractDBService {

    @Value("${address.contract_download}")
    private String ADDRESS_CONTRACT_DOWNLOAD;

    private final MarketDataVerticle marketDataVerticle;

    @Autowired
    public UpdateContractDBService(MarketDataVerticle marketDataVerticle) {
        this.marketDataVerticle = marketDataVerticle;
    }

    /**
     * Service entry point.
     */
    public void update() {
        VertxOptions options = new VertxOptions();
        options.setBlockedThreadCheckInterval(1000);
        options.setMaxWorkerExecuteTime(5000000000L);
        final Vertx vertx = Vertx.vertx(options);
        Observable<String> marketDataDeployment = RxHelper.deployVerticle(vertx, marketDataVerticle);
        marketDataDeployment.flatMap(deploymentId -> {
            log.info("succesfully deployed ContractInfo verticle: " + deploymentId);
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
                log.error("failed to load resource: ", e);
                vertx.close();
            }
            return codesStream;
        })
                .concatMap(code -> Observable.just(code).delay(100, TimeUnit.MILLISECONDS))  // throttling
                .flatMap(new ContractFetcher(vertx, ADDRESS_CONTRACT_DOWNLOAD))
                .doOnNext(result -> {
                    JsonObject error = result.body().getJsonObject("error");
                    if (!error.equals(MarketDataVerticle.EMPTY)) {
                        log.error("error occured:" + error);
                    }
                })
                .subscribe(response -> {
                    log.info("processed: " + response.body());
                }, failed -> {
                    log.error("terminating - unrecoverable error occurred:", failed);
                    System.exit(0);
                }, () -> {
                    log.info("completed");
                    System.exit(0);
                });
    }

    /**
     * Created by Christophe on 04/01/2017.
     */
    private static class ContractFetcher implements Func1<String, Observable<? extends Message<JsonObject>>> {


        private final String addressContractDownload;
        private final Vertx vertx;

        public ContractFetcher(Vertx vertx, String addressContractDownload) {
            this.addressContractDownload = addressContractDownload;
            this.vertx = vertx;
        }

        @Override
        public Observable<? extends Message<JsonObject>> call(String code) {
            JsonObject contractCode = new JsonObject().put("conId", code);
            ObservableFuture<Message<JsonObject>> contractStream =
                    io.vertx.rx.java.RxHelper.observableFuture();
            DeliveryOptions deliveryOptions = new DeliveryOptions();
            deliveryOptions.setSendTimeout(10000);
            vertx.eventBus().send(addressContractDownload, contractCode,
                    deliveryOptions, contractStream.toHandler());
            return contractStream;
        }
    }
}
