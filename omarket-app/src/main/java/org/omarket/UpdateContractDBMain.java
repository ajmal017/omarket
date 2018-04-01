package org.omarket;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.core.RxHelper;
import io.vertx.rxjava.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import org.omarket.trading.ContractFetcher;
import org.omarket.trading.verticles.MarketDataVerticle;
import org.omarket.trading.verticles.VerticleProperties;
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

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@Slf4j
public class UpdateContractDBMain {

    public static void main(String[] args) throws IOException {
        SpringApplication.run(UpdateContractDBMain.class, args);
        
        final Vertx vertx = Vertx.vertx();
        DeploymentOptions options;
        if(args.length == 0){
            int defaultClientId = 2;
            options = VerticleProperties.makeDeploymentOptions(defaultClientId);
        } else {
            options = VerticleProperties.makeDeploymentOptions(args[0]);
        }
        Observable<String> marketDataDeployment = RxHelper.deployVerticle(vertx, new MarketDataVerticle(), options);
        marketDataDeployment.flatMap(deploymentId -> {
            log.info("succesfully deployed market data verticle: " + deploymentId);
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
                .flatMap(new ContractFetcher(vertx))
                .doOnNext(result -> {
                    JsonObject error = result.body().getJsonObject("error");
                    if (!error.equals(MarketDataVerticle.EMPTY)) {
                        log.error("error occured:" + error);
                    }
                })
                .subscribe(response -> {
                    log.info("processed: " + response);
                }, failed -> {
                    log.error("terminating - unrecoverable error occurred:", failed);
                    System.exit(0);
                }, () -> {
                    log.info("completed");
                    System.exit(0);
                });
    }

}
