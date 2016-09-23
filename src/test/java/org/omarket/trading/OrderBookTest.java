package org.omarket.trading;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertNull;

/**
 * Created by christophe on 23.09.16.
 */
public class OrderBookTest {
    private static Logger logger = LoggerFactory.getLogger(OrderBookTest.class);

    @Test
    public void basicOrderBookUsage() {
        OrderBook orderBook = new OrderBook();

        orderBook.newBid(new BigDecimal("99"), 12);
        orderBook.newBid(new BigDecimal("98"), 10);
        orderBook.newBid(new BigDecimal("99"), 3);
        orderBook.newBid(new BigDecimal("97"), 5);
        orderBook.newBid(new BigDecimal("100"), 2);

        orderBook.newAsk(new BigDecimal("101"), 21);
        orderBook.newAsk(new BigDecimal("102"), 9);
        orderBook.newAsk(new BigDecimal("101"), 9);

        List<Pair<BigDecimal, Integer>> bidOrderLevels = orderBook.getBidOrderLevels();
        logger.info("bid side:");
        int counter = 0;
        for(Pair<BigDecimal, Integer> level: bidOrderLevels){
            counter++;
            logger.info("level {}: {}", counter, level);
        }
        List<Pair<BigDecimal, Integer>> askOrderLevels = orderBook.getAskOrderLevels();
        logger.info("ask side:");
        counter = 0;
        for(Pair<BigDecimal, Integer> level: askOrderLevels){
            counter++;
            logger.info("level {}: {}", counter, level);
        }

        logger.info("best bid: {}", orderBook.getBestBid());
        logger.info("best ask: {}", orderBook.getBestAsk());
        // assert statements
        //assertEquals("10 x 0 must be 0", 0, tester.multiply(10, 0));
    }

    @Test(expected=java.lang.AssertionError.class)
    public void wrongBidAsk() {
        OrderBook orderBook = new OrderBook();

        orderBook.newBid(new BigDecimal("99"), 12);
        orderBook.newBid(new BigDecimal("98"), 10);
        orderBook.newBid(new BigDecimal("99"), 3);
        orderBook.newBid(new BigDecimal("100"), 2);

        orderBook.newAsk(new BigDecimal("101"), 21);
        orderBook.newAsk(new BigDecimal("100"), 9);

    }

    @Test
    public void emptyBook() {
        OrderBook orderBook = new OrderBook();
        assertNull(orderBook.getBestBid());
        assertNull(orderBook.getBestAsk());
    }
}