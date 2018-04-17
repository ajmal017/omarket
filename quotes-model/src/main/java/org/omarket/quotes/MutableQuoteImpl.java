package org.omarket.quotes;

import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

/**
 * Created by Christophe on 04/11/2016.
 * <p>
 * Implementation of a MutableQuote interface.
 */
@Slf4j
class MutableQuoteImpl extends QuoteImpl implements MutableQuote {

    private BigDecimal minTick;

    MutableQuoteImpl(BigDecimal minTick, String productCode) {
        super(null, null, null, null, null, productCode);
        this.minTick = minTick;
    }

    @Override
    public boolean updateBestBidSize(int size) {
        if (this.getBestBidSize() != null && this.getBestBidSize().equals(size)) {
            return false;
        }
        this.bestBidSize = size;
        setLastModified(ZonedDateTime.now(ZoneOffset.UTC));
        return true;
    }

    @Override
    public boolean updateBestAskSize(int size) {
        if (this.getBestAskSize() != null && this.getBestAskSize() == size) {
            return false;
        }
        this.bestAskSize = size;
        setLastModified(ZonedDateTime.now(ZoneOffset.UTC));
        return true;
    }

    @Override
    public boolean updateBestBidPrice(double price) {
        BigDecimal newBestBidPrice = BigDecimal.valueOf(Double.valueOf(price / minTick.doubleValue()).intValue()).multiply(minTick);
        if (newBestBidPrice.equals(this.getBestBidPrice())) {
            return false;
        }
        this.bestBidPrice = newBestBidPrice;
        setLastModified(ZonedDateTime.now(ZoneOffset.UTC));
        return true;
    }

    @Override
    public boolean updateBestAskPrice(double price) {
        BigDecimal newBestAskPrice = BigDecimal.valueOf(Double.valueOf(price / minTick.doubleValue()).intValue()).multiply(minTick);
        if (newBestAskPrice.equals(this.getBestAskPrice())) {
            return false;
        }
        this.bestAskPrice = newBestAskPrice;
        setLastModified(ZonedDateTime.now(ZoneOffset.UTC));
        return true;
    }

    private void setLastModified(ZonedDateTime lastModified) {
        this.lastModified = lastModified;
    }

}
