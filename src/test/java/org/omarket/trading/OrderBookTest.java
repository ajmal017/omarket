package org.omarket.trading;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
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

        assertEquals("best bid does not match", new ImmutablePair<>(new BigDecimal(100), 2), orderBook.getBestBid());
        assertEquals("best ask does not match", new ImmutablePair<>(new BigDecimal(101), 30), orderBook.getBestAsk());
    }

    @Test(expected=java.lang.AssertionError.class)
    public void wrongBidEqualsAsk() {
        OrderBook orderBook = new OrderBook();

        orderBook.newBid(new BigDecimal("99"), 12);
        orderBook.newBid(new BigDecimal("98"), 10);
        orderBook.newBid(new BigDecimal("99"), 3);
        orderBook.newBid(new BigDecimal("100"), 2);

        orderBook.newAsk(new BigDecimal("101"), 21);
        orderBook.newAsk(new BigDecimal("100"), 9);
    }

    @Test(expected=java.lang.AssertionError.class)
    public void wrongBidHigherThanAsk() {
        OrderBook orderBook = new OrderBook();

        orderBook.newBid(new BigDecimal("99"), 12);
        orderBook.newBid(new BigDecimal("98"), 10);
        orderBook.newBid(new BigDecimal("99"), 3);
        orderBook.newBid(new BigDecimal("100"), 2);

        orderBook.newAsk(new BigDecimal("101"), 21);
        orderBook.newAsk(new BigDecimal("90"), 9);
    }

    @Test
    public void deleteOrder() {
        OrderBook orderBook = new OrderBook();

        String orderId1 = orderBook.newBid(new BigDecimal("99"), 12);
        String orderId2 = orderBook.newBid(new BigDecimal("98"), 10);
        String orderId3 = orderBook.newBid(new BigDecimal("99"), 3);
        String orderId4 = orderBook.newBid(new BigDecimal("97"), 5);
        String orderId5 = orderBook.newBid(new BigDecimal("100"), 2);

        String orderId6 = orderBook.newAsk(new BigDecimal("101"), 21);
        String orderId7 = orderBook.newAsk(new BigDecimal("102"), 9);
        String orderId8 = orderBook.newAsk(new BigDecimal("101"), 9);
        orderBook.deleteOrder(orderId8);

        assertEquals("best bid does not match", new ImmutablePair<>(new BigDecimal(100), 2), orderBook.getBestBid());
        assertEquals("best ask does not match", new ImmutablePair<>(new BigDecimal(101), 21), orderBook.getBestAsk());

        assertEquals(4, orderBook.getBidOrderLevels().size());
        assertEquals(2, orderBook.getAskOrderLevels().size());

        orderBook.deleteOrder(orderId1);
        orderBook.deleteOrder(orderId2);
        orderBook.deleteOrder(orderId3);
        orderBook.deleteOrder(orderId4);
        orderBook.deleteOrder(orderId5);
        orderBook.deleteOrder(orderId6);

        assertNull(orderBook.getBestBid());
        assertEquals("best ask does not match", new ImmutablePair<>(new BigDecimal(102), 9), orderBook.getBestAsk());
        assertEquals(0, orderBook.getBidOrderLevels().size());
        assertEquals(1, orderBook.getAskOrderLevels().size());
        orderBook.deleteOrder(orderId7);
        assertNull(orderBook.getBestAsk());
    }

    @Test
    public void emptyBook() {
        OrderBook orderBook = new OrderBook();
        assertNull(orderBook.getBestBid());
        assertNull(orderBook.getBestAsk());
    }
}