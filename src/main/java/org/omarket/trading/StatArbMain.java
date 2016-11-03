package org.omarket.trading;

import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.omarket.trading.verticles.MarketDataVerticle;
import org.omarket.trading.verticles.StrategyVerticle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatArbMain {
    private static Logger logger = LoggerFactory.getLogger(StatArbMain.class);

    public static void main(String[] args) throws InterruptedException {
        float default_start_value = 100;
        Integer period = 1000;
        Vertx vertx = Vertx.vertx();
        DeploymentOptions options = new DeploymentOptions()
                .setConfig(new JsonObject().put("fakequote1.startValue", default_start_value)
                );
        //vertx.deployVerticle(new FakeQuoteGeneratorVerticle("quotesource.fakeQuote1", period, new BigDecimal(100), new BigDecimal("0.1")), options);
        //vertx.deployVerticle(new LoggerVerticle("log1", "quotesource.fakeQuote1"), options);
        //vertx.deployVerticle(new LoggerVerticle("log2", "quotesource.fakeQuote1"), options);


        final Handler<AsyncResult<String>> marketDataCompletionHandler = result -> {
            if (result.succeeded()) {
                //
                // Main code - begin
                //
                logger.info("market data deployment result: " + result.result());
                final JsonObject product = new JsonObject().put("conId", "12334");

                vertx.eventBus().send(MarketDataVerticle.ADDRESS_CONTRACT_DETAILS, product, reply -> {
                    if (reply.succeeded()) {
                        JsonObject contractDetails = (JsonObject)reply.result().body();
                        logger.info("received contract details: " + contractDetails);
                        vertx.eventBus().send(MarketDataVerticle.ADDRESS_SUBSCRIBE, contractDetails, mktDataReply -> {
                            logger.info("subscription result: " + mktDataReply.result().body());
                        });

                    } else {
                        logger.error("failed to retrieve contract details");
                    }
                });

                //
                // Main code - end
                //
            } else {
                logger.error("failed to deploy: " + result);
            }
        };
        MarketDataVerticle marketDataVerticle = new MarketDataVerticle();
        vertx.deployVerticle(marketDataVerticle, marketDataCompletionHandler);

        StrategyVerticle strategyVerticle = new StrategyVerticle();
        vertx.deployVerticle(strategyVerticle);

        logger.info("deployment completed");

    }
}
