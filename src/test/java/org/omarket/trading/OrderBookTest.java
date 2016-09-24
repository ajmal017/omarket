package org.omarket.trading;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Created by christophe on 23.09.16.
 */
public class OrderBookTest {
    private static Logger logger = LoggerFactory.getLogger(OrderBookTest.class);

    @Before
    public void setUp() throws Exception {

    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void basicOrderBookUsage() throws Exception {
        OrderBook orderBook = new OrderBook();

        orderBook.newBid(new BigDecimal("99"), 12);
        orderBook.newBid(new BigDecimal("98"), 10);
        orderBook.newBid(new BigDecimal("99"), 3);
        orderBook.newBid(new BigDecimal("97"), 5);
        orderBook.newBid(new BigDecimal("100"), 2);

        orderBook.newAsk(new BigDecimal("101"), 21);
        orderBook.newAsk(new BigDecimal("102"), 9);
        orderBook.newAsk(new BigDecimal("101"), 9);

        assertNull("bid does not match", orderBook.getBidLevel(4));
        assertEquals("bid does not match", new ImmutablePair<>(new BigDecimal(97), 5), orderBook.getBidLevel(3));
        assertEquals("bid does not match", new ImmutablePair<>(new BigDecimal(98), 10), orderBook.getBidLevel(2));
        assertEquals("bid does not match", new ImmutablePair<>(new BigDecimal(99), 15), orderBook.getBidLevel(1));
        assertEquals("bid does not match", new ImmutablePair<>(new BigDecimal(100), 2), orderBook.getBidLevel(0));

        assertNull("ask does not match", orderBook.getAskLevel(2));
        assertEquals("ask does not match", new ImmutablePair<>(new BigDecimal(102), 9), orderBook.getAskLevel(1));
        assertEquals("ask does not match", new ImmutablePair<>(new BigDecimal(101), 30), orderBook.getAskLevel(0));

        assertEquals("best bid does not match", new BigDecimal(100), orderBook.getBestBid());
        assertEquals("best ask does not match", new BigDecimal(101), orderBook.getBestAsk());
    }

    @Test(expected=java.lang.AssertionError.class)
    public void wrongBidEqualsAsk() throws Exception {
        OrderBook orderBook = new OrderBook();

        orderBook.newBid(new BigDecimal("99"), 12);
        orderBook.newBid(new BigDecimal("98"), 10);
        orderBook.newBid(new BigDecimal("99"), 3);
        orderBook.newBid(new BigDecimal("100"), 2);

        orderBook.newAsk(new BigDecimal("101"), 21);
        orderBook.newAsk(new BigDecimal("100"), 9);
    }

    @Test(expected=java.lang.AssertionError.class)
    public void wrongBidHigherThanAsk() throws Exception {
        OrderBook orderBook = new OrderBook();

        orderBook.newBid(new BigDecimal("99"), 12);
        orderBook.newBid(new BigDecimal("98"), 10);
        orderBook.newBid(new BigDecimal("99"), 3);
        orderBook.newBid(new BigDecimal("100"), 2);

        orderBook.newAsk(new BigDecimal("101"), 21);
        orderBook.newAsk(new BigDecimal("90"), 9);
    }

    @Test
    public void deleteOrder() throws Exception {
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

        assertEquals("best bid does not match", new ImmutablePair<>(new BigDecimal(100), 2), orderBook.getBidLevel(0));
        assertEquals("best ask does not match", new ImmutablePair<>(new BigDecimal(101), 21), orderBook.getAskLevel(0));

        assertEquals(4, orderBook.getBidOrderLevels().size());
        assertEquals(2, orderBook.getAskOrderLevels().size());

        orderBook.deleteOrder(orderId1);
        orderBook.deleteOrder(orderId2);
        orderBook.deleteOrder(orderId3);

        assertNull("bid does not match", orderBook.getBidLevel(2));
        assertEquals("bid does not match", new ImmutablePair<>(new BigDecimal(97), 5), orderBook.getBidLevel(1));
        assertEquals("bid does not match", new ImmutablePair<>(new BigDecimal(100), 2), orderBook.getBidLevel(0));

        orderBook.deleteOrder(orderId4);
        orderBook.deleteOrder(orderId5);
        orderBook.deleteOrder(orderId6);

        assertNull(orderBook.getBidLevel(0));
        assertEquals("best ask does not match", new ImmutablePair<>(new BigDecimal(102), 9), orderBook.getAskLevel(0));
        assertEquals(0, orderBook.getBidOrderLevels().size());
        assertEquals(1, orderBook.getAskOrderLevels().size());
        orderBook.deleteOrder(orderId7);
        assertNull(orderBook.getAskLevel(0));
    }

    @Test
    public void emptyBook() throws Exception {
        OrderBook orderBook = new OrderBook();
        assertNull(orderBook.getBidLevel(0));
        assertNull(orderBook.getAskLevel(0));
    }
    @Test
    public void orderBookUpdate() throws Exception {
        OrderBook orderBook = new OrderBook();
        Date lastUpdate0 = orderBook.getLastUpdate();
        assertNull(lastUpdate0);
        String orderId1 = orderBook.newBid(new BigDecimal("99"), 12);
        String orderId2 = orderBook.newBid(new BigDecimal("98"), 10);
        String orderId3 = orderBook.newBid(new BigDecimal("99"), 3);
        String orderId4 = orderBook.newBid(new BigDecimal("97"), 5);
        String orderId5 = orderBook.newBid(new BigDecimal("100"), 2);

        String orderId6 = orderBook.newAsk(new BigDecimal("101"), 21);
        String orderId7 = orderBook.newAsk(new BigDecimal("102"), 9);
        String orderId8 = orderBook.newAsk(new BigDecimal("101"), 9);

        Date lastUpdate1 = orderBook.getLastUpdate();
        assertNotNull(lastUpdate1);
        orderBook.updateOrder(orderId6, 31);
        Date lastUpdate2 = orderBook.getLastUpdate();
        assert(lastUpdate2.compareTo(lastUpdate1) >= 0);

        assertEquals("best bid does not match", new ImmutablePair<>(new BigDecimal(100), 2), orderBook.getBidLevel(0));
        assertEquals("best ask does not match", new ImmutablePair<>(new BigDecimal(101), 40), orderBook.getAskLevel(0));

        orderBook.deleteOrder(orderId1);
        orderBook.deleteOrder(orderId2);
        orderBook.deleteOrder(orderId3);
        orderBook.deleteOrder(orderId4);
        orderBook.deleteOrder(orderId5);
        orderBook.deleteOrder(orderId6);
        orderBook.deleteOrder(orderId7);
        orderBook.deleteOrder(orderId8);

        assertNull(orderBook.getBidLevel(0));
        assertNull(orderBook.getAskLevel(0));

    }

}