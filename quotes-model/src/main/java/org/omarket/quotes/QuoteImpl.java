package org.omarket.quotes;

import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalUnit;


/**
 * Created by Christophe on 04/11/2016.
 * <p>
 * Implementation of Quote interface.
 */
@Slf4j
class QuoteImpl implements Quote {

    ZonedDateTime lastModified = null;
    BigDecimal bestBidPrice = null;
    BigDecimal bestAskPrice = null;
    Integer bestBidSize = null;
    Integer bestAskSize = null;
    private String productCode = null;

    QuoteImpl(ZonedDateTime lastModified, Integer bestBidSize, BigDecimal bestBidPrice, BigDecimal bestAskPrice, Integer bestAskSize, String productCode) {
        this.lastModified = lastModified;
        this.bestBidPrice = bestBidPrice;
        this.bestAskPrice = bestAskPrice;
        this.bestBidSize = bestBidSize;
        this.bestAskSize = bestAskSize;
        this.productCode = productCode;
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

    @Override
    public boolean isValid() {
        boolean isNull = bestBidPrice == null || bestAskPrice == null || bestBidSize == null || bestAskSize == null;
        return !isNull && bestBidSize > 0 && bestAskSize > 0 && bestBidPrice.compareTo(bestAskPrice) < 0;
    }

    @Override
    public boolean sameSampledTime(Quote other, TemporalUnit temporalUnit) {
        return getLastModified().truncatedTo(temporalUnit).equals(other.getLastModified().truncatedTo(temporalUnit));
    }

    @Override
    public String getProductCode() {
        return productCode;
    }

    public String toString() {
        String timestamp = DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(getLastModified());
        return "< " + getBestBidSize() + " " + getBestBidPrice() + " / " + getBestAskPrice() + " " + getBestAskSize() + " > (" + timestamp + " - " + getProductCode() + ")";
    }

}
