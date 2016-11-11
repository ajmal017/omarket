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
public class OrderBookLevelOne {

    private static Logger logger = LoggerFactory.getLogger(OrderBookLevelOne.class);

    private Date lastModified = null;
    private BigDecimal bestBidPrice = null;
    private BigDecimal bestAskPrice = null;
    private Integer bestBidSize = null;
    private Integer bestAskSize = null;
    private int decimalPrecision;
    private final SimpleDateFormat millisFormat;
    private final static SimpleDateFormat isoFormat;

    static {
        isoFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss:SSS'Z'");
        isoFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    public OrderBookLevelOne(double minTick) {
        String[] parts = String.format(Locale.ROOT,"%f", minTick).split("\\.");
        if (parts[0].equals("0")) {
            decimalPrecision = parts[parts.length - 1].length();
        } else {
            decimalPrecision = 0;
        }
        millisFormat = new SimpleDateFormat("mm:ss.SSS");
        millisFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    public boolean updateBestBidSize(int size) {
        if (bestBidSize != null && bestBidSize.equals(size)){
            return false;
        }
        bestBidSize = size;
        setLastModified(new Date());
        return true;
    }

    public boolean updateBestAskSize(int size) {
        if (bestAskSize != null && bestAskSize == size){
            return false;
        }
        bestAskSize = size;
        setLastModified(new Date());
        return true;
    }

    public boolean updateBestBidPrice(double price) {
        BigDecimal newBestBidPrice = BigDecimal.valueOf(price).setScale(decimalPrecision, BigDecimal.ROUND_HALF_UP);
        if (newBestBidPrice.equals(bestBidPrice)){
            return false;
        }
        bestBidPrice = newBestBidPrice;
        setLastModified(new Date());
        return true;
    }

    public boolean updateBestAskPrice(double price) {
        BigDecimal newBestAskPrice = BigDecimal.valueOf(price).setScale(decimalPrecision, BigDecimal.ROUND_HALF_UP);
        if (newBestAskPrice.equals(bestAskPrice)){
            return false;
        }
        bestAskPrice = newBestAskPrice;
        setLastModified(new Date());
        return true;
    }

    private void setLastModified(Date lastModified) {
        this.lastModified = lastModified;
    }

    public Date getLastModified() {
        return lastModified;
    }

    public BigDecimal getBestBidPrice() {
        return bestBidPrice;
    }

    public BigDecimal getBestAskPrice() {
        return bestAskPrice;
    }

    public Integer getBestBidSize() {
        return bestBidSize;
    }

    public Integer getBestAskSize() {
        return bestAskSize;
    }

    public String toString() {
        return "< " + getBestBidSize() + " " + getBestBidPrice() + " / " + getBestAskPrice() + " " + getBestAskSize() + " >";
    }

    public boolean isValid() {
        boolean isNull = bestBidPrice == null || bestAskPrice == null|| bestBidSize == null|| bestAskSize == null;
        return !isNull && bestBidSize > 0 && bestAskSize > 0 && bestBidPrice.compareTo(bestAskPrice) < 0;
    }

    public JsonObject asJSON() {
        JsonObject asJSON = new JsonObject();
        BigDecimal bidPrice = getBestBidPrice();
        BigDecimal askPrice = getBestAskPrice();
        asJSON.put("lastModified", isoFormat.format(getLastModified()));
        asJSON.put("bestBidSize", getBestBidSize());
        if (bidPrice!=null){
            asJSON.put("bestBidPrice", bidPrice.doubleValue());
        } else {
            asJSON.put("bestBidPrice", (Enum)null);
        }
        if (askPrice!=null){
            asJSON.put("bestAskPrice", askPrice.doubleValue());
        } else {
            asJSON.put("bestAskPrice", (Enum)null);
        }
        asJSON.put("bestAskSize", getBestAskSize());
        return asJSON;
    }

    public String asPriceVolumeString() {
        return this.millisFormat.format(getLastModified()) + "," + getBestBidSize() + "," + getBestBidPrice() + "," + getBestAskPrice() + "," + getBestAskSize();
    }

    public static OrderBookLevelOne fromJSON(JsonObject json, double minTick) throws ParseException {
        OrderBookLevelOne newOrderBook = new OrderBookLevelOne(minTick);
        newOrderBook.lastModified = isoFormat.parse(json.getString("lastModified"));
        newOrderBook.bestBidPrice = BigDecimal.valueOf(json.getDouble("bestBidPrice"));
        newOrderBook.bestAskPrice = BigDecimal.valueOf(json.getDouble("bestAskPrice"));
        newOrderBook.bestBidSize = json.getInteger("bestBidSize");
        newOrderBook.bestAskSize = json.getInteger("bestAskSize");
        return newOrderBook;
    }
}
