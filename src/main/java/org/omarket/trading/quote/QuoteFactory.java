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

    public static QuoteImpl createFrom(Quote quote, ChronoUnit sampleUnit){
        return createFrom(quote, sampleUnit, 0);
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
        return new QuoteImpl(lastModified.truncatedTo(sampleUnit).plus(1, sampleUnit), bestBidSize, bestBidPrice, bestAskPrice, bestAskSize, productCode);
    }

    public static QuoteImpl createFrom(Quote quote, ChronoUnit sampleUnit, Integer sampleDelay){
        logger.info("reference time: " + quote.getLastModified());
        ZonedDateTime lastModified = quote.getLastModified().truncatedTo(sampleUnit).plus(sampleDelay + 1, sampleUnit);
        logger.info("target time: " + lastModified);
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
