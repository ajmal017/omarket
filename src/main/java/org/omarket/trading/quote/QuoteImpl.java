package org.omarket.trading.quote;

import org.apache.commons.lang3.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.*;

import static org.omarket.trading.quote.Quote.Sampling.HOUR;
import static org.omarket.trading.quote.Quote.Sampling.MINUTE;

/**
 * Created by Christophe on 04/11/2016.
 *
 * Implementation of Quote interface.
 */
class QuoteImpl implements Quote {

    private static Logger logger = LoggerFactory.getLogger(QuoteImpl.class);

    Date lastModified = null;
    BigDecimal bestBidPrice = null;
    BigDecimal bestAskPrice = null;
    Integer bestBidSize = null;
    Integer bestAskSize = null;

    QuoteImpl(Date lastModified, Integer bestBidSize, BigDecimal bestBidPrice, BigDecimal bestAskPrice, Integer bestAskSize) {
        this.lastModified = lastModified;
        this.bestBidPrice = bestBidPrice;
        this.bestAskPrice = bestAskPrice;
        this.bestBidSize = bestBidSize;
        this.bestAskSize = bestAskSize;
    }

    @Override
    public Date getLastModified() {
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
        return "< " + getBestBidSize() + " " + getBestBidPrice() + " / " + getBestAskPrice() + " " + getBestAskSize() + " >";
    }

    @Override
    public boolean isValid() {
        boolean isNull = bestBidPrice == null || bestAskPrice == null|| bestBidSize == null|| bestAskSize == null;
        return !isNull && bestBidSize > 0 && bestAskSize > 0 && bestBidPrice.compareTo(bestAskPrice) < 0;
    }

    @Override
    public boolean sameSampledTime(Quote other, Sampling frequency){
        Date timestamp;
        Date timestampOther;
        if (frequency == HOUR){
            timestamp = DateUtils.truncate(getLastModified(), Calendar.HOUR);
            timestampOther = DateUtils.truncate(other.getLastModified(), Calendar.HOUR);
        } else if (frequency == MINUTE){
            timestamp = DateUtils.truncate(getLastModified(), Calendar.MINUTE);
            timestampOther = DateUtils.truncate(other.getLastModified(), Calendar.MINUTE);
        } else {
            timestamp = DateUtils.truncate(getLastModified(), Calendar.SECOND);
            timestampOther = DateUtils.truncate(other.getLastModified(), Calendar.SECOND);
        }
        return timestamp.equals(timestampOther);
    }

}
