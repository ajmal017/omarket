package org.omarket.trading;

import org.junit.Test;

import java.math.BigDecimal;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by christophe on 23.09.16.
 */
public class OrderBookTest {
    private static Logger logger = LoggerFactory.getLogger(OrderBookTest.class);

    @Test
    public void basicOrderBookUsage() {
        OrderBook orderBook = new OrderBook();
        orderBook.newBid(new BigDecimal("99"), 12);
        orderBook.newAsk(new BigDecimal("100"), 21);
        TreeMap<BigDecimal, TreeMap<String, Order>> bidSide = orderBook.getBidSide();
        logger.info("bid side:");
        int counter = 0;
        for(BigDecimal price: bidSide.keySet()){
            counter++;
            logger.info("level {}: {}", counter, price);
            TreeMap<String, Order> orders = bidSide.get(price);
            logger.info("content: {}", orders);
        }

        logger.info("ask side: {}", orderBook.getAskSide());
        // assert statements
        //assertEquals("10 x 0 must be 0", 0, tester.multiply(10, 0));
    }
}