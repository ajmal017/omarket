package org.omarket.trading.quote;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.math.BigDecimal;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

/**
 * Created by Christophe on 07/12/2016.
 */
public class QuoteFactory {
    private final static Logger logger = LoggerFactory.getLogger(QuoteFactory.class);

    public static QuoteImpl create(ZonedDateTime lastModified, Integer bestBidSize, BigDecimal bestBidPrice, BigDecimal bestAskPrice, Integer bestAskSize, String productCode){
        return new QuoteImpl(lastModified, bestBidSize, bestBidPrice, bestAskPrice, bestAskSize, productCode);
    }

    public static QuoteImpl createFrom(Quote quote, ZonedDateTime lastModified){
        Integer bestBidSize = quote.getBestBidSize();
        BigDecimal bestBidPrice = quote.getBestBidPrice();
        BigDecimal bestAskPrice = quote.getBestAskPrice();
        Integer bestAskSize = quote.getBestAskSize();
        String productCode = quote.getProductCode();
        return new QuoteImpl(lastModified, bestBidSize, bestBidPrice, bestAskPrice, bestAskSize, productCode);
    }

    public static QuoteImpl createFrom(Quote quote, ZonedDateTime lastModified, ChronoUnit sampleUnit){
        Integer bestBidSize = quote.getBestBidSize();
        BigDecimal bestBidPrice = quote.getBestBidPrice();
        BigDecimal bestAskPrice = quote.getBestAskPrice();
        Integer bestAskSize = quote.getBestAskSize();
        String productCode = quote.getProductCode();
        return new QuoteImpl(toSampledTime(lastModified, sampleUnit), bestBidSize, bestBidPrice, bestAskPrice, bestAskSize, productCode);
    }

    public static ZonedDateTime toSampledTime(ZonedDateTime dateTime, ChronoUnit sampleUnit){
        ZonedDateTime truncated = dateTime.truncatedTo(sampleUnit);
        ZonedDateTime output;
        if (truncated.equals(dateTime)) {
            output = dateTime;
        } else {
            output = truncated.plus(1, sampleUnit);
        }
        return output;
    }

    public static QuoteImpl createFrom(Quote quote, ChronoUnit sampleUnit, Integer sampleDelay){
        ZonedDateTime lastModified = toSampledTime(quote.getLastModified(), sampleUnit).plus(sampleDelay, sampleUnit);
        Integer bestBidSize = quote.getBestBidSize();
        BigDecimal bestBidPrice = quote.getBestBidPrice();
        BigDecimal bestAskPrice = quote.getBestAskPrice();
        Integer bestAskSize = quote.getBestAskSize();
        String productCode = quote.getProductCode();
        return new QuoteImpl(lastModified, bestBidSize, bestBidPrice, bestAskPrice, bestAskSize, productCode);
    }

    public static MutableQuoteImpl createMutable(BigDecimal minTick, String productCode) {
        return new MutableQuoteImpl(minTick, productCode);
    }

    public static MutableQuoteImpl createMutable(String minTick, String productCode) {
        return new MutableQuoteImpl(new BigDecimal(minTick), productCode);
    }
}
