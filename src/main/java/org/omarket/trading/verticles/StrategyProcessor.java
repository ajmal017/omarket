package org.omarket.trading.verticles;

import org.omarket.trading.quote.Quote;
import java.text.ParseException;

/**
 * Created by christophe on 30/11/16.
 */
public interface StrategyProcessor extends QuoteProcessor {
    void processQuote(Quote quote);
}
