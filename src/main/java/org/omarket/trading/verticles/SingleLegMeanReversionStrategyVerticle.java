package org.omarket.trading.verticles;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.omarket.trading.verticles.MarketDataVerticle.createChannelOrderBookLevelOne;


/**
 * Created by Christophe on 01/11/2016.
 */
public class SingleLegMeanReversionStrategyVerticle extends AbstractVerticle {
    private final static Logger logger = LoggerFactory.getLogger(SingleLegMeanReversionStrategyVerticle.class);

    private static JsonObject contract;

    public void start() {
        logger.info("starting strategy verticle");
        final Integer productEurChf = 12087817;

        MarketDataVerticle.subscribeProduct(vertx, productEurChf, reply -> {
            if (reply.succeeded()) {
                logger.info("subscribed to:" + productEurChf);
                contract = reply.result().body();
            } else {
                logger.error("failed to subscribe to: " + productEurChf);
            }
        });
        String channelProduct = createChannelOrderBookLevelOne(productEurChf);
        vertx.eventBus().consumer(channelProduct, (Message<JsonObject> message) -> orderBookReceived(contract, message.body()));
    }

    private static void orderBookReceived(JsonObject contract, JsonObject orderBook) {
        String symbol = contract.getJsonObject("m_contract").getString("m_localSymbol");
        logger.debug("received for " + symbol + ": " + orderBook);
        // TODO: calc signal, run from recorded ticks...
    }
}
