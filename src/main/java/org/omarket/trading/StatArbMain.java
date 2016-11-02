package org.omarket.trading;

import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.omarket.trading.verticles.ContractDetailsVerticle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Thread.sleep;

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
        logger.info("deploying contract details verticle");
        Handler<AsyncResult<String>> completionHandler = result -> {
            if (result.succeeded()) {
                logger.info("contract details deployment result: " + result.result());

                final JsonObject product = new JsonObject()
                        .put("code", "IBM")
                        .put("exchange", "SMART")
                        .put("currency", "USD");

                vertx.eventBus().send(ContractDetailsVerticle.ADDRESS, product, reply -> {
                    if (reply.succeeded()) {
                        Object replyMessage = reply.result().body();
                        logger.info("received reply: " + replyMessage);
                    } else {
                        logger.error("failed to retrieve contract details");
                    }
                });
            } else {
                logger.error("failed to deploy: " + result);
            }
        };
        ContractDetailsVerticle contractDetailsVerticle = new ContractDetailsVerticle();
        vertx.deployVerticle(contractDetailsVerticle, completionHandler);

        logger.info("deployment completed");

    }
}
