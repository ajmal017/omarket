package org.omarket.trading;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.omarket.trading.quote.MutableQuote;
import org.omarket.trading.quote.QuoteFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

/**
 * Created by Christophe on 04/11/2016.
 *
 * Testing a mutable quote.
 *
 */
public class MutableQuoteImplTest {

    private static Logger logger = LoggerFactory.getLogger(MutableQuoteImplTest.class);

    @Before
    public void setUp() throws Exception {

    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void basicUsagePrecision2() throws Exception {
        MutableQuote orderBook = QuoteFactory.createMutable("0.02");
        orderBook.updateBestBidPrice(10.125);
        orderBook.updateBestAskPrice(10.159);
        assertEquals("unexpected order book", "< null 10.12 / 10.14 null >", orderBook.toString().substring(0, 27));
    }

    @Test
    public void basicUsagePrecision0() throws Exception {
        MutableQuote orderBook = QuoteFactory.createMutable("1");
        orderBook.updateBestBidPrice(10.125);
        orderBook.updateBestAskPrice(10.139);
        assertEquals("unexpected order book", "< null 10 / 10 null >", orderBook.toString().substring(0, 21));
    }
}
