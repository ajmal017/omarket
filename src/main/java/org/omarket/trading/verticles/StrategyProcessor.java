package org.omarket.trading.verticles;

import org.omarket.trading.OrderBookLevelOneImmutable;

/**
 * Created by christophe on 30/11/16.
 */
public interface StrategyProcessor {
    void processOrderBook(OrderBookLevelOneImmutable orderBook, boolean isBacktest);
}
