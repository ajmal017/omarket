package org.omarket.trading.verticles;

import io.vertx.rx.java.ObservableFuture;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.omarket.trading.MarketData.createChannelQuote;


/**
 * Created by Christophe on 01/11/2016.
 */
public class DummyStrategyVerticle extends AbstractVerticle {
    private final static Logger logger = LoggerFactory.getLogger(DummyStrategyVerticle.class);

    private static Map<Integer, JsonObject> orderBooks = new HashMap<>();
    private static Map<Integer, JsonObject> contracts = new HashMap<>();

    public void start() {
        logger.info("starting strategy verticle");
        // Global X Copper Miners ETF - COPX - 211651700
        // PowerShares DB Oil Fund - DBO - 42393358
        final Integer productCopperETF = 211651700;
        final Integer productOilETF = 42393358;
        Integer[] ibCodes = {productCopperETF, productOilETF};

        for (Integer ibCode : ibCodes) {
            ObservableFuture<Message<JsonObject>> observable = MarketDataVerticle.subscribeProduct(vertx, ibCode);
            observable.subscribe(
                    message -> {
                    logger.info("subscribed to:" + ibCode);
                    JsonObject contractDetails = message.body();
                    contracts.put(ibCode, contractDetails);
                }, failure-> {
                    logger.error("failed to subscribe to: " + ibCode);
                }
            );
            String channelProduct = createChannelQuote(ibCode);
            vertx.eventBus().consumer(channelProduct, (Message<JsonObject> message) -> orderBookReceived(ibCode, message.body()));
        }
    }

    private static void orderBookReceived(Integer productCode, JsonObject message) {
        JsonObject contract = contracts.get(productCode);
        orderBooks.put(productCode, message);
        String symbol = contract.getJsonObject("m_contract").getString("m_localSymbol");
        logger.debug("received for " + symbol + ": " + message);
        // TODO: calc signal, deploy web server, run from recorded ticks...
    }
}
