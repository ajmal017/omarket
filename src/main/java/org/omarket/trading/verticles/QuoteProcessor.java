package org.omarket.trading.verticles;

import io.vertx.core.json.JsonObject;
import joinery.DataFrame;
import org.omarket.trading.quote.Quote;

import java.util.Deque;
import java.util.Map;

/**
 * Interface for processing quotes.
 */
public interface QuoteProcessor {
    /**
     * Called back every time a new tick is sent.
     *
     * @param contracts mapping each contract code to the relevant contract properties
     * @param quotes tick data, in increasing order of timestamp (last is most recent)
     * @param sampledQuotes sampled data
     */
    void processQuotes(Map<String, JsonObject> contracts, Map<String, Deque<Quote>> quotes, Map<String, DataFrame> sampledQuotes);
}
