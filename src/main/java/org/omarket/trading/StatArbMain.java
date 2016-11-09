package org.omarket.trading;

import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.omarket.trading.ibrokers.CurrencyProduct;
import org.omarket.trading.verticles.LoggerVerticle;
import org.omarket.trading.verticles.MarketDataVerticle;
import org.omarket.trading.verticles.StrategyVerticle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.omarket.trading.verticles.MarketDataVerticle.createChannelOrderBookLevelOne;

public class StatArbMain {
    private final static Logger logger = LoggerFactory.getLogger(StatArbMain.class);

    public static void main(String[] args) throws InterruptedException {
        String defaultStoragePath = "ticks";
        int defaultClientId = 1;
        String defaultHost = "127.0.0.1";
        int defaultPort = 7497;
        DeploymentOptions options = new DeploymentOptions()
                .setConfig(new JsonObject()
                        .put("ibrokers.ticks.storagePath", defaultStoragePath)
                        .put("ibrokers.clientId", defaultClientId)
                        .put("ibrokers.host", defaultHost)
                        .put("ibrokers.port", defaultPort)
                );

        final Vertx vertx = Vertx.vertx();

        final Handler<AsyncResult<String>> marketDataCompletionHandler = result -> {
            if (result.succeeded()) {
                //
                // Main code - begin
                //

                logger.info("market data deployment result: " + result.result());
                String[] trackedCurrencies = new String[]{
                        "GBP.USD", "AUD.USD", "EUR.USD", "USD.JPY",
                        "NZD.USD", "USD.SGD", "USD.SEK", "USD.CHF",
                        "USD.RUB", "USD.NOK", "USD.MXN", "USD.MXN",
                        "USD.ILS", "USD.HUF", "USD.HKD", "USD.CZK",
                        "USD.CNH", "USD.CAD"
                };
                for (String currencyCross: trackedCurrencies) {
                    Integer ibCode = CurrencyProduct.IB_CODES.get(currencyCross);
                    MarketDataVerticle.subscribeProduct(vertx, ibCode);
                }

                //
                // Main code - end
                //
            } else {
                logger.error("failed to deploy: " + result.cause());
            }
        };
        vertx.deployVerticle(MarketDataVerticle.class.getName(), options, marketDataCompletionHandler);
        vertx.deployVerticle(StrategyVerticle.class.getName());

        vertx.setPeriodic(3000, id -> {
            MarketDataVerticle.adminCommand(vertx, "subscribed");
            MarketDataVerticle.adminCommand(vertx, "help");
            MarketDataVerticle.adminCommand(vertx, "details 42393358");
        });

        logger.info("deployment completed");

    }
}
