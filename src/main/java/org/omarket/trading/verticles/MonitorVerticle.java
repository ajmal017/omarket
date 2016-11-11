package org.omarket.trading.verticles;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpServer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

/**
 * Created by Christophe on 11/11/2016.
 */
public class MonitorVerticle  extends AbstractVerticle {
    private final static Logger logger = LoggerFactory.getLogger(MonitorVerticle.class);
    public void start() {
        HttpServer server = vertx.createHttpServer();
        String host = config().getString("oot.monitor.host", "0.0.0.0");
        Integer port = config().getInteger("oot.monitor.port", 8080);
        server.requestHandler(request -> {
            // Handle the request in here
            request.response().end("Hello world");
        });
        server.listen(port, host);
    }
}
