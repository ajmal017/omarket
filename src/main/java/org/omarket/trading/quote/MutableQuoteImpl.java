package org.omarket.trading.quote;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Locale;

/**
 * Created by Christophe on 04/11/2016.
 *
 * Implementation of a MutableQuote interface.
 *
 */
class MutableQuoteImpl extends QuoteImpl implements MutableQuote {

    private static Logger logger = LoggerFactory.getLogger(MutableQuoteImpl.class);

    private int decimalPrecision;

    MutableQuoteImpl(double minTick) {
        super(null, null, null, null, null);
        String[] parts = String.format(Locale.ROOT,"%f", minTick).split("\\.");
        if (parts[0].equals("0")) {
            decimalPrecision = parts[parts.length - 1].length();
        } else {
            decimalPrecision = 0;
        }
    }

    @Override
    public boolean updateBestBidSize(int size) {
        if (this.getBestBidSize() != null && this.getBestBidSize().equals(size)){
            return false;
        }
        this.bestBidSize = size;
        setLastModified(ZonedDateTime.now(ZoneOffset.UTC));
        return true;
    }

    @Override
    public boolean updateBestAskSize(int size) {
        if (this.getBestAskSize() != null && this.getBestAskSize() == size){
            return false;
        }
        this.bestAskSize = size;
        setLastModified(ZonedDateTime.now(ZoneOffset.UTC));
        return true;
    }

    @Override
    public boolean updateBestBidPrice(double price) {
        BigDecimal newBestBidPrice = BigDecimal.valueOf(price).setScale(decimalPrecision, BigDecimal.ROUND_HALF_UP);
        if (newBestBidPrice.equals(this.getBestBidPrice())){
            return false;
        }
        this.bestBidPrice = newBestBidPrice;
        setLastModified(ZonedDateTime.now(ZoneOffset.UTC));
        return true;
    }

    @Override
    public boolean updateBestAskPrice(double price) {
        BigDecimal newBestAskPrice = BigDecimal.valueOf(price).setScale(decimalPrecision, BigDecimal.ROUND_HALF_UP);
        if (newBestAskPrice.equals(this.getBestAskPrice())){
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
