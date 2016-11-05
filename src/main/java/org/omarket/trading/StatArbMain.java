package org.omarket.trading;

import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.omarket.trading.verticles.LoggerVerticle;
import org.omarket.trading.verticles.MarketDataVerticle;
import org.omarket.trading.verticles.StrategyVerticle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ContractDetailsReplyHandler implements Handler<AsyncResult<Message<JsonObject>>> {

    @Override
    public void handle(AsyncResult<Message<JsonObject>> event) {

    }
}

public class StatArbMain {
    private final static Logger logger = LoggerFactory.getLogger(StatArbMain.class);

    public static void main(String[] args) throws InterruptedException {
        int default_client_id = 1;
        String default_host = "127.0.0.1";
        int default_port = 7497;
        DeploymentOptions options = new DeploymentOptions()
                .setConfig(new JsonObject()
                        .put("ibrokers.clientId", default_client_id)
                        .put("ibrokers.host", default_host)
                        .put("ibrokers.port", default_port)
                );

        final Vertx vertx = Vertx.vertx();

        final Handler<AsyncResult<String>> marketDataCompletionHandler = result -> {
            if (result.succeeded()) {
                //
                // Main code - begin
                //
                logger.info("market data deployment result: " + result.result());
                // Global X Copper Miners ETF - COPX - 211651700
                // PowerShares DB Oil Fund - DBO - 42393358
                final Integer product_copper_etf = 211651700;
                final Integer product_oil_etf = 42393358;
                Integer[] ibCodes = {product_copper_etf, product_oil_etf};
                for (Integer ibCode : ibCodes) {
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
        vertx.deployVerticle(new LoggerVerticle("COPX", "oot.orderBookLevelOne.211651700"));
        vertx.deployVerticle(new LoggerVerticle("DBO", "oot.orderBookLevelOne.42393358"));
        logger.info("deployment completed");

    }
}
