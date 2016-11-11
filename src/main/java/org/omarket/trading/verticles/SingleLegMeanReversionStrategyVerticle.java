package org.omarket.trading.verticles;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.omarket.trading.OrderBookLevelOne;

import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

import static org.omarket.trading.verticles.MarketDataVerticle.createChannelOrderBookLevelOne;


/**
 * Created by Christophe on 01/11/2016.
 */
public class SingleLegMeanReversionStrategyVerticle extends AbstractVerticle {
    private final static Logger logger = LoggerFactory.getLogger(SingleLegMeanReversionStrategyVerticle.class);
    private final static ArrayBlockingQueue<OrderBookLevelOne> orderBookHistory = new ArrayBlockingQueue<OrderBookLevelOne>(1000);
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
        vertx.eventBus().consumer(channelProduct, (Message<JsonObject> message) -> {
            double minTick = contract.getDouble("m_minTick");
            try {
                orderBookReceived(contract, OrderBookLevelOne.fromJSON(message.body(), minTick));
            } catch (ParseException e) {
                logger.error("failed to parse tick data for contract " + contract, e);
            }
        });
    }

    private static void orderBookReceived(JsonObject contract, OrderBookLevelOne orderBook) {
        String symbol = contract.getJsonObject("m_contract").getString("m_localSymbol");
        logger.debug("received for " + symbol + ": " + orderBook);
        // TODO: calc signal, run from recorded ticks...
    }
}
