package org.omarket.trading;

import io.vertx.core.*;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TestVerticle extends AbstractVerticle {
    final Logger logger = LoggerFactory.getLogger(TestVerticle.class.getName());

    public static final String ADDRESS = "oot.test";

    public void start() {
        logger.info("starting test verticle");
        MessageConsumer<JsonObject> consumer = vertx.eventBus().consumer(ADDRESS);
        consumer.handler(message -> {
            final JsonObject body = message.body();
            logger.info("received: " + body);
            JsonObject replyMessage = body.copy();
            replyMessage.put("status", "processed");
            message.reply(replyMessage);
        });
        logger.info("started test verticle");
    }
}
public class VertxMain {
    private final static Logger logger = LoggerFactory.getLogger(VertxMain.class);

    public static void main(String[] args) throws InterruptedException {
        Vertx vertx = Vertx.vertx();
        logger.info("deploying test verticle");
        Handler<AsyncResult<String>> completionHandler = result -> {
            System.out.println("done");
            if (result.succeeded()) {
                logger.info("contract details deployment result: " + result.result());
            } else {
                logger.error("failed to deploy: " + result);
            }
        };
        TestVerticle testVerticle = new TestVerticle();
        vertx.deployVerticle(testVerticle, completionHandler);

        logger.info("deployment completed");
    }
}
