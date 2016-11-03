package org.omarket.trading.verticles;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Christophe on 01/11/2016.
 */
public class MarketDataVerticle extends AbstractVerticle{
    Logger logger = LoggerFactory.getLogger(MarketDataVerticle.class.getName());

    public static final String ADDRESS_SUBSCRIBE = "oot.marketData.subscribe";
    public static final String ADDRESS_SUBSCRIBE_MULTIPLE = "oot.marketData.subscribeMultiple";
    public static final String ADDRESS_UNSUBSCRIBE = "oot.marketData.unsubscribe";
    public static final String ADDRESS_UNSUBSCRIBE_MULTIPLE = "oot.marketData.unsubscribeMultiple";
    public static final String ADDRESS_UNSUBSCRIBE_ALL = "oot.marketData.unsubscribeAll";
    public static final String ADDRESS_CONTRACT_DETAILS = "oot.marketData.contractDetails";
    private Map<String, JsonObject> subscribedProducts = new HashMap<>();

    public void start() {
        logger.info("starting market data verticle");
        MessageConsumer<JsonObject> consumerSubscribe = vertx.eventBus().consumer(ADDRESS_SUBSCRIBE);
        consumerSubscribe.handler(message -> {
            final JsonObject contractDetails = message.body();
            logger.info("received subscription request for: " + contractDetails);
            String status = "failed";
            String productCode = contractDetails.getString("conId");
            if(!subscribedProducts.containsKey(productCode)){
                // subscription takes place here
                subscribedProducts.put(productCode, contractDetails);
                status = "registered";
            } else {
                status = "already_registered";
            }
            final JsonObject reply = new JsonObject().put("status", status);
            message.reply(reply);
        });

        MessageConsumer<JsonObject> consumerUnsubscribe = vertx.eventBus().consumer(ADDRESS_UNSUBSCRIBE);
        consumerUnsubscribe.handler(message -> {
            final JsonObject contract = message.body();
            logger.info("received unsubscription request for: " + contract);
            String status = "failed";
            String productCode = contract.getString("conId");
            if(subscribedProducts.containsKey(productCode)){
                // un-subscription takes place here
                subscribedProducts.remove(productCode);
                status = "unsubscribed";
            } else {
                status = "missing";
            }
            final JsonObject reply = new JsonObject().put("status", status);
            message.reply(reply);
        });
        logger.info("started market data verticle");

        MessageConsumer<JsonObject> consumer = vertx.eventBus().consumer(ADDRESS_CONTRACT_DETAILS);
        consumer.handler(message -> {
            final JsonObject body = message.body();
            logger.info("received: " + body);
            String productCode = body.getString("conId");
            // lookup product code and return details
            final JsonObject product = new JsonObject()
                    .put("code", "IBM")
                    .put("exchange", "SMART")
                    .put("currency", "USD")
                    .put("conId", productCode);
            message.reply(product);
        });
    }
}
