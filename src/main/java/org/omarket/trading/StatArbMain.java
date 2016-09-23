package org.omarket.trading;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

import java.math.BigDecimal;

public class StatArbMain {

    public static void main(String[] args) throws InterruptedException {
        float default_start_value = 100;
        Integer period = 1000;
        Vertx vertx = Vertx.vertx();
        DeploymentOptions options = new DeploymentOptions()
                .setConfig(new JsonObject().put("fakequote1.startValue", default_start_value)
                );
        vertx.deployVerticle(new FakeQuoteGeneratorVerticle("quotesource.fakeQuote1", period, new BigDecimal(100), new BigDecimal("0.1")), options);
        vertx.deployVerticle(new LoggerVerticle("log1", "quotesource.fakeQuote1"), options);
        vertx.deployVerticle(new LoggerVerticle("log2", "quotesource.fakeQuote1"), options);
    }
}
