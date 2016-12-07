package org.omarket.trading.verticles;

import org.omarket.trading.quote.Quote;
import java.text.ParseException;

/**
 * Created by christophe on 30/11/16.
 */
public interface StrategyProcessor {
    void processQuote(Quote quote, boolean isBacktest);
    void updateQuotes(Quote quote) throws ParseException;
}
