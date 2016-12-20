package org.omarket.trading.verticles;

import org.omarket.trading.quote.Quote;

import java.util.Map;
import java.util.Queue;

/**
 * Created by christophe on 15/12/16.
 */
public interface QuoteProcessor {
    void processQuotes(Map<String, Quote> latestQuotes, Map<String, Queue<Quote>> sampledQuotes);
}
