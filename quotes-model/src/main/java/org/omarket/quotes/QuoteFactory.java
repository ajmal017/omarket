package org.omarket.quotes;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

/**
 * Created by Christophe on 07/12/2016.
 */
@Slf4j
@Service
public class QuoteFactory {

    public QuoteImpl create(ZonedDateTime lastModified, Integer bestBidSize, BigDecimal bestBidPrice, BigDecimal bestAskPrice, Integer bestAskSize, String productCode) {
        return new QuoteImpl(lastModified, bestBidSize, bestBidPrice, bestAskPrice, bestAskSize, productCode);
    }

    public QuoteImpl createFrom(Quote quote, ZonedDateTime lastModified) {
        Integer bestBidSize = quote.getBestBidSize();
        BigDecimal bestBidPrice = quote.getBestBidPrice();
        BigDecimal bestAskPrice = quote.getBestAskPrice();
        Integer bestAskSize = quote.getBestAskSize();
        String productCode = quote.getProductCode();
        return new QuoteImpl(lastModified, bestBidSize, bestBidPrice, bestAskPrice, bestAskSize, productCode);
    }

    public QuoteImpl createFrom(Quote quote, ZonedDateTime lastModified, ChronoUnit sampleUnit) {
        Integer bestBidSize = quote.getBestBidSize();
        BigDecimal bestBidPrice = quote.getBestBidPrice();
        BigDecimal bestAskPrice = quote.getBestAskPrice();
        Integer bestAskSize = quote.getBestAskSize();
        String productCode = quote.getProductCode();
        return new QuoteImpl(toSampledTime(lastModified, sampleUnit), bestBidSize, bestBidPrice, bestAskPrice, bestAskSize, productCode);
    }

    public ZonedDateTime toSampledTime(ZonedDateTime dateTime, ChronoUnit sampleUnit) {
        ZonedDateTime truncated = dateTime.truncatedTo(sampleUnit);
        ZonedDateTime output;
        if (truncated.equals(dateTime)) {
            output = dateTime;
        } else {
            output = truncated.plus(1, sampleUnit);
        }
        return output;
    }

    public QuoteImpl createFrom(Quote quote, ChronoUnit sampleUnit, Integer sampleDelay) {
        ZonedDateTime lastModified = toSampledTime(quote.getLastModified(), sampleUnit).plus(sampleDelay, sampleUnit);
        Integer bestBidSize = quote.getBestBidSize();
        BigDecimal bestBidPrice = quote.getBestBidPrice();
        BigDecimal bestAskPrice = quote.getBestAskPrice();
        Integer bestAskSize = quote.getBestAskSize();
        String productCode = quote.getProductCode();
        return new QuoteImpl(lastModified, bestBidSize, bestBidPrice, bestAskPrice, bestAskSize, productCode);
    }

    public MutableQuoteImpl createMutable(BigDecimal minTick, String productCode) {
        return new MutableQuoteImpl(minTick, productCode);
    }

    public MutableQuoteImpl createMutable(String minTick, String productCode) {
        return new MutableQuoteImpl(new BigDecimal(minTick), productCode);
    }
}
