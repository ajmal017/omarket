package org.omarket.trading.verticles;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

/**
 * Created by Christophe on 01/11/2016.
 */
public class ContractDetailsVerticle extends AbstractVerticle{
    Logger logger = LoggerFactory.getLogger(ContractDetailsVerticle.class.getName());

    public static final String ADDRESS = "oot.contractDetails";

    public void start() {
        logger.info("starting contract details verticle");
        MessageConsumer<JsonObject> consumer = vertx.eventBus().consumer(ADDRESS);
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
        logger.info("started contract details verticle");
    }
}