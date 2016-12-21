package org.omarket.trading.verticles;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.StaticHandler;
import io.vertx.ext.web.handler.sockjs.BridgeOptions;
import io.vertx.ext.web.handler.sockjs.PermittedOptions;
import io.vertx.ext.web.handler.sockjs.SockJSHandler;

/**
 * Created by Christophe on 11/11/2016.
 */
public class MonitorVerticle  extends AbstractVerticle {
    private final static Logger logger = LoggerFactory.getLogger(MonitorVerticle.class);
    public static final String ADDRESS_MONITOR_STRATEGY = "oot.monitor.strategy";

    public void start() {
        Router router = Router.router(vertx);
        BridgeOptions bridgeOptions = new BridgeOptions();
        PermittedOptions permittedOptions = new PermittedOptions();
        permittedOptions.setAddress(ADDRESS_MONITOR_STRATEGY);
        bridgeOptions.addOutboundPermitted(permittedOptions);

        router.route("/oot/*").handler(SockJSHandler.create(vertx).bridge(bridgeOptions));
        router.route().handler(StaticHandler.create());

        HttpServer server = vertx.createHttpServer();
        server.requestHandler(router::accept);
        String host = config().getString("oot.monitor.host", "0.0.0.0");
        Integer port = config().getInteger("oot.monitor.port", 8080);
        server.listen(port, host);

        vertx.eventBus().consumer(DummyMeanReversionStrategyVerticle.ADDRESS_STRATEGY_SIGNAL, (Message<JsonObject> message) -> {
            Double signal = message.body().getDouble("signal");
            Double threshold1 = message.body().getDouble("thresholdLow1");
            JsonObject newSample = new JsonObject()
                    .put("time", System.currentTimeMillis())
                    .put("signal", signal)
                    .put("thresholdLow1", threshold1);
            vertx.eventBus().publish(ADDRESS_MONITOR_STRATEGY, newSample);
        });

    }
}
