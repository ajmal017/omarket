package org.omarket.trading.verticles;

import org.omarket.trading.quote.Quote;

/**
 * Created by christophe on 15/12/16.
 */
public interface QuoteProcessor {

    void processQuote(Quote quote);
}
