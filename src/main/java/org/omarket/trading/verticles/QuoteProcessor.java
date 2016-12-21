package org.omarket.trading.verticles;

import org.omarket.trading.quote.Quote;

import java.util.Deque;
import java.util.List;
import java.util.Map;

/**
 * Created by christophe on 15/12/16.
 */
public interface QuoteProcessor {
    void processQuotes(Map<String, Quote> latestQuotes, Map<String, Deque<Quote>> quotes, Map<String, Deque<Quote>> sampledQuotes);
}
