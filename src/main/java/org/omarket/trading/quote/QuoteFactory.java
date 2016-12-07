package org.omarket.trading.quote;

import java.math.BigDecimal;
import java.time.ZonedDateTime;

/**
 * Created by Christophe on 07/12/2016.
 */
public class QuoteFactory {
    public static QuoteImpl create(ZonedDateTime lastModified, Integer bestBidSize, BigDecimal bestBidPrice, BigDecimal bestAskPrice, Integer bestAskSize){
        return new QuoteImpl(lastModified, bestBidSize, bestBidPrice, bestAskPrice, bestAskSize);
    }

    public static MutableQuoteImpl createMutable(double minTick) {
        return new MutableQuoteImpl(minTick);
    }
}
