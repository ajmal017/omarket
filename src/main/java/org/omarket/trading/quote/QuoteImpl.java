package org.omarket.trading.quote;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalUnit;


/**
 * Created by Christophe on 04/11/2016.
 *
 * Implementation of Quote interface.
 */
class QuoteImpl implements Quote {

    private static Logger logger = LoggerFactory.getLogger(QuoteImpl.class);

    ZonedDateTime lastModified = null;
    BigDecimal bestBidPrice = null;
    BigDecimal bestAskPrice = null;
    Integer bestBidSize = null;
    Integer bestAskSize = null;

    QuoteImpl(ZonedDateTime lastModified, Integer bestBidSize, BigDecimal bestBidPrice, BigDecimal bestAskPrice, Integer bestAskSize) {
        this.lastModified = lastModified;
        this.bestBidPrice = bestBidPrice;
        this.bestAskPrice = bestAskPrice;
        this.bestBidSize = bestBidSize;
        this.bestAskSize = bestAskSize;
    }

    @Override
    public ZonedDateTime getLastModified() {
        return lastModified;
    }

    @Override
    public BigDecimal getBestBidPrice() {
        return bestBidPrice;
    }

    @Override
    public BigDecimal getBestAskPrice() {
        return bestAskPrice;
    }

    @Override
    public Integer getBestBidSize() {
        return bestBidSize;
    }

    @Override
    public Integer getBestAskSize() {
        return bestAskSize;
    }

    public String toString() {
        String timestamp = DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(getLastModified());
        return "< " + getBestBidSize() + " " + getBestBidPrice() + " / " + getBestAskPrice() + " " + getBestAskSize() + " > (" + timestamp + ")";
    }

    @Override
    public boolean isValid() {
        boolean isNull = bestBidPrice == null || bestAskPrice == null|| bestBidSize == null|| bestAskSize == null;
        return !isNull && bestBidSize > 0 && bestAskSize > 0 && bestBidPrice.compareTo(bestAskPrice) < 0;
    }

    @Override
    public boolean sameSampledTime(Quote other, TemporalUnit temporalUnit){
        return getLastModified().truncatedTo(temporalUnit).equals(other.getLastModified().truncatedTo(temporalUnit));
    }

}
