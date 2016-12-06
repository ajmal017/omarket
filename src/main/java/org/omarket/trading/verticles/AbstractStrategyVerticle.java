package org.omarket.trading.verticles;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.omarket.trading.MarketData;
import org.omarket.trading.OrderBookLevelOneImmutable;

import java.text.ParseException;
import java.util.*;

import static org.omarket.trading.verticles.MarketDataVerticle.IBROKERS_TICKS_STORAGE_PATH;
import static org.omarket.trading.MarketData.createChannelOrderBookLevelOne;

/**
 * Created by Christophe on 18/11/2016.
 */

abstract class AbstractStrategyVerticle extends AbstractVerticle implements StrategyProcessor {
    private final static Logger logger = LoggerFactory.getLogger(AbstractStrategyVerticle.class);

    private OrderBookLevelOneImmutable orderBook;

    private JsonObject parameters = new JsonObject();

    private Map<Integer, JsonObject> contracts = new HashMap<>();

    abstract protected Integer[] getIBrokersCodes();

    /**
     * For how long back the verticle needs to keep past order books.
     *
     * @return lookback period in milliseconds
     */
    abstract protected Integer getLookBackPeriod();

    /*
     *
     */
    abstract protected void init(Integer lookbackPeriod);

    public void updateOrderBooks(OrderBookLevelOneImmutable orderBookPrev){
        // TODO
    }

    protected JsonObject getParameters(){
        return parameters;
    }
    protected List<OrderBookLevelOneImmutable> getPastOrderBooks(){
        JsonArray orderBooks = getParameters().getJsonArray("pastOrderBooks");
        List<OrderBookLevelOneImmutable> orderBooksList = orderBooks.getList();
        return orderBooksList;
    }

    @Override
    public void start() {
        vertx.executeBlocking(future -> {
            try {
                JsonArray array = new JsonArray();
                getParameters().put("pastOrderBooks", array);
                init(getLookBackPeriod());
                future.complete();
            } catch (Exception e) {
                logger.error("failed to initialize strategy", e);
                future.fail(e);
            }
        }, completed -> {
            logger.info("initialized strategy");
            for (Integer ibCode: getIBrokersCodes()) {
                vertx.executeBlocking(future -> {
                    boolean runBacktest = config().getBoolean("runBacktestFlag", true);
                    if(runBacktest) {
                        logger.info("executing backtest");
                        JsonArray storageDirs = config().getJsonArray(IBROKERS_TICKS_STORAGE_PATH);
                        List<String> dirs = storageDirs.getList();
                        try {
                            MarketData.processBacktest(dirs, ibCode, this);
                            future.complete();
                        } catch (Exception e) {
                            logger.error("backtest failed", e);
                            future.fail(e);
                        }
                    } else {
                        logger.info("skipping backtest");
                        future.complete();
                    }
                }, result -> {
                    logger.info("processing realtime ticks for " + ibCode);
                    // now launching realtime ticks
                    MarketDataVerticle.subscribeProduct(vertx, ibCode, subscribeProductResult -> {
                        logger.info("subscription received for product: " + ibCode);
                        if (subscribeProductResult.succeeded()) {
                            logger.info("subscription succeeded for product: " + ibCode);
                            JsonObject contract = subscribeProductResult.result().body();
                            contracts.put(ibCode, contract);
                            // Constantly maintains order book up to date
                            final String channelProduct = createChannelOrderBookLevelOne(ibCode);
                            vertx.eventBus().consumer(channelProduct, (Message<JsonObject> message) -> {
                                try {
                                    orderBook = OrderBookLevelOneImmutable.fromJSON(message.body());
                                    logger.info("updated order book: " + orderBook);
                                } catch (ParseException e) {
                                    logger.error("failed to parse tick data for contract " + contract, e);
                                }
                            });

                        } else {
                            logger.error("failed to subscribe to: " + ibCode);
                        }
                    });
                });
            }

            vertx.setPeriodic(1000, id -> {
                if(contracts.size() != getIBrokersCodes().length || orderBook == null){
                    return;
                }
                // Sampling: calculates signal
                logger.info("processing order book: " + orderBook);
                processOrderBook(orderBook, false);
                updateOrderBooks(orderBook);
            });
        });

    }

}

