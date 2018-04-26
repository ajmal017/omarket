package org.omarket.quotes;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.assertEquals;

/**
 * Created by Christophe on 04/11/2016.
 * <p>
 * Testing a mutable quotes.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@ContextConfiguration(classes = {MutableQuoteImplTest.MutableQuoteConfig.class})
public class MutableQuoteImplTest {

    @Autowired
    private QuoteFactory quoteFactory;

    @Configuration
    @ComponentScan(basePackages = {"org.omarket.quotes"})
    protected static class MutableQuoteConfig {
    }

    @Before
    public void setUp() throws Exception {

    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void basicUsagePrecision2() throws Exception {
        MutableQuote orderBook = quoteFactory.createMutable("0.02", "test000");
        orderBook.updateBestBidPrice(10.125);
        orderBook.updateBestAskPrice(10.159);
        assertEquals("unexpected order book", "< null 10.12 / 10.14 null >", orderBook.toString().substring(0, 27));
    }

    @Test
    public void basicUsagePrecision0() throws Exception {
        MutableQuote orderBook = quoteFactory.createMutable("1", "test000");
        orderBook.updateBestBidPrice(10.125);
        orderBook.updateBestAskPrice(10.139);
        assertEquals("unexpected order book", "< null 10 / 10 null >", orderBook.toString().substring(0, 21));
    }
}
