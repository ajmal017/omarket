package org.omarket.trading;

import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.omarket.trading.verticles.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static org.omarket.trading.verticles.MarketDataVerticle.IBROKERS_TICKS_STORAGE_PATH;

public class StatArbMain {
    private final static Logger logger = LoggerFactory.getLogger(StatArbMain.class);

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

        final Handler<AsyncResult<String>> marketDataCompletionHandler = result -> {
            if (result.succeeded()) {
                //
                // Main code - begin
                //
                vertx.deployVerticle(SingleLegMeanReversionStrategyVerticle.class.getName(), options);
                //
                // Main code - end
                //
            } else {
                logger.error("failed to deploy", result.cause());
            }
        };
        vertx.deployVerticle(FakeMarketDataVerticle.class.getName(), options, marketDataCompletionHandler);
        //vertx.deployVerticle(MonitorVerticle.class.getName(), options);

        logger.info("deployment completed");

    }
}
