package org.omarket.trading.verticles;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;


/**
 * Created by Christophe on 01/11/2016.
 */
public class StrategyVerticle extends AbstractVerticle{
    Logger logger = LoggerFactory.getLogger(StrategyVerticle.class.getName());

    public void start() {
        logger.info("starting strategy verticle");
    }
}
