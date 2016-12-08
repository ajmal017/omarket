package org.omarket.trading;

import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.RxHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

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
        TestVerticle testVerticle = new TestVerticle();
        Observable<String> deployment = RxHelper.deployVerticle(vertx, testVerticle);
        deployment.subscribe(id -> {
            // Deployed
            logger.info("deployment completed: " + id);
        }, err -> {
            // Could not deploy
            logger.error("failed to deploy: " + err);
        });
    }
}
