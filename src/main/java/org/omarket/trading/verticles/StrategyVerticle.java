package org.omarket.trading.verticles;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import static org.omarket.trading.verticles.MarketDataVerticle.createChannelOrderBookLevelOne;


/**
 * Created by Christophe on 01/11/2016.
 */
public class StrategyVerticle extends AbstractVerticle{
    private final static Logger logger = LoggerFactory.getLogger(StrategyVerticle.class);

    public void start() {
        logger.info("starting strategy verticle");
        // Global X Copper Miners ETF - COPX - 211651700
        // PowerShares DB Oil Fund - DBO - 42393358
        final Integer productCopperETF = 211651700;
        final Integer productOilETF = 42393358;
        Integer[] ibCodes = {productCopperETF, productOilETF};
        for (Integer ibCode : ibCodes) {
            MarketDataVerticle.subscribeProduct(vertx, ibCode);
        }

        String channelCOPX = createChannelOrderBookLevelOne(productCopperETF);
        String channelDBO = createChannelOrderBookLevelOne(productOilETF);
        vertx.eventBus().consumer(channelCOPX, (Message<JsonObject> message) -> orderBookReceived(productCopperETF, message.body()));
        vertx.eventBus().consumer(channelDBO, (Message<JsonObject> message) -> orderBookReceived(productOilETF, message.body()));
    }

    private static void orderBookReceived(Integer productCode, JsonObject message) {
        logger.info("received for {}: {}", productCode, message);
    }
}
