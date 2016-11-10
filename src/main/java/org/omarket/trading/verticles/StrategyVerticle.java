package org.omarket.trading.verticles;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.omarket.trading.OrderBookLevelOne;

import java.util.HashMap;
import java.util.Map;

import static org.omarket.trading.verticles.MarketDataVerticle.createChannelOrderBookLevelOne;


/**
 * Created by Christophe on 01/11/2016.
 */
public class StrategyVerticle extends AbstractVerticle {
    private final static Logger logger = LoggerFactory.getLogger(StrategyVerticle.class);

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
            MarketDataVerticle.subscribeProduct(vertx, ibCode, reply -> {
                if (reply.succeeded()) {
                    logger.info("subscribed to:" + ibCode);
                    JsonObject contractDetails = reply.result().body();
                    contracts.put(ibCode, contractDetails);
                } else {
                    logger.error("failed to subscribe to: " + ibCode);
                }
            });
            String channelProduct = createChannelOrderBookLevelOne(ibCode);
            vertx.eventBus().consumer(channelProduct, (Message<JsonObject> message) -> orderBookReceived(ibCode, message.body()));
        }
    }

    private static void orderBookReceived(Integer productCode, JsonObject message) {
        JsonObject contract = contracts.get(productCode);
        orderBooks.put(productCode, message);
        String symbol = contract.getJsonObject("m_contract").getString("m_localSymbol");
        logger.info("received for " + symbol + ": " + message);
        // TODO: calc signal, deploy web server, ...
    }
}
