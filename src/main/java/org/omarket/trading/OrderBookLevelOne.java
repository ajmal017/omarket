package org.omarket.trading;

import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

/**
 * Created by Christophe on 04/11/2016.
 */
public class OrderBookLevelOne extends OrderBookLevelOneImmutable {

    private static Logger logger = LoggerFactory.getLogger(OrderBookLevelOne.class);

    private int decimalPrecision;

    public OrderBookLevelOne(double minTick) {
        super(null, null, null, null, null);
        String[] parts = String.format(Locale.ROOT,"%f", minTick).split("\\.");
        if (parts[0].equals("0")) {
            decimalPrecision = parts[parts.length - 1].length();
        } else {
            decimalPrecision = 0;
        }
    }

    public boolean updateBestBidSize(int size) {
        if (this.getBestBidSize() != null && this.getBestBidSize().equals(size)){
            return false;
        }
        this.bestBidSize = size;
        setLastModified(new Date());
        return true;
    }

    public boolean updateBestAskSize(int size) {
        if (this.getBestAskSize() != null && this.getBestAskSize() == size){
            return false;
        }
        this.bestAskSize = size;
        setLastModified(new Date());
        return true;
    }

    public boolean updateBestBidPrice(double price) {
        BigDecimal newBestBidPrice = BigDecimal.valueOf(price).setScale(decimalPrecision, BigDecimal.ROUND_HALF_UP);
        if (newBestBidPrice.equals(this.getBestBidPrice())){
            return false;
        }
        this.bestBidPrice = newBestBidPrice;
        setLastModified(new Date());
        return true;
    }

    public boolean updateBestAskPrice(double price) {
        BigDecimal newBestAskPrice = BigDecimal.valueOf(price).setScale(decimalPrecision, BigDecimal.ROUND_HALF_UP);
        if (newBestAskPrice.equals(this.getBestAskPrice())){
            return false;
        }
        this.bestAskPrice = newBestAskPrice;
        setLastModified(new Date());
        return true;
    }

    private void setLastModified(Date lastModified) {
        this.lastModified = lastModified;
    }

}
