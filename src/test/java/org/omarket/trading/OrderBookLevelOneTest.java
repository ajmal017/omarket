package org.omarket.trading;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

/**
 * Created by Christophe on 04/11/2016.
 */
public class OrderBookLevelOneTest {

    private static Logger logger = LoggerFactory.getLogger(OrderBookLevelOneTest.class);

    @Before
    public void setUp() throws Exception {

    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void basicUsagePrecision2() throws Exception {
        OrderBookLevelOne orderBook = new OrderBookLevelOne(0.01);
        orderBook.updateBestBidPrice(10.125);
        orderBook.updateBestAskPrice(10.139);
        assertEquals("unexpected order book", "< null 10.13 / 10.14 null >", orderBook.toString());
    }

    @Test
    public void basicUsagePrecision0() throws Exception {
        OrderBookLevelOne orderBook = new OrderBookLevelOne(1);
        orderBook.updateBestBidPrice(10.125);
        orderBook.updateBestAskPrice(10.139);
        assertEquals("unexpected order book", "< null 10 / 10 null >", orderBook.toString());
    }
}
