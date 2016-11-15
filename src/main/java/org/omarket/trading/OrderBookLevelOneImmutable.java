package org.omarket.trading;

import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.omarket.trading.OrderBookLevelOneImmutable.Sampling.HOUR;
import static org.omarket.trading.OrderBookLevelOneImmutable.Sampling.MINUTE;
import static org.omarket.trading.OrderBookLevelOneImmutable.Sampling.SECOND;

/**
 * Created by Christophe on 04/11/2016.
 */
public class OrderBookLevelOneImmutable {

    private static Logger logger = LoggerFactory.getLogger(OrderBookLevelOneImmutable.class);

    protected Date lastModified = null;
    protected BigDecimal bestBidPrice = null;
    protected BigDecimal bestAskPrice = null;
    protected Integer bestBidSize = null;
    protected Integer bestAskSize = null;
    private final static SimpleDateFormat millisFormat;
    protected final static SimpleDateFormat isoFormat;
    public enum Sampling {
        SECOND, MINUTE, HOUR
    }

    static {
        isoFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss:SSS'Z'");
        isoFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        millisFormat = new SimpleDateFormat("mm:ss.SSS");
        millisFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    public OrderBookLevelOneImmutable(Date lastModified, Integer bestBidSize, BigDecimal bestBidPrice, BigDecimal bestAskPrice, Integer bestAskSize) {
        this.lastModified = lastModified;
        this.bestBidPrice = bestBidPrice;
        this.bestAskPrice = bestAskPrice;
        this.bestBidSize = bestBidSize;
        this.bestAskSize = bestAskSize;
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

    public boolean sameSampledTime(OrderBookLevelOneImmutable other, Sampling frequency){
        Date timestamp;
        Date timestampOther;
        if (frequency == HOUR){
            timestamp = DateUtils.round(getLastModified(), Calendar.HOUR);
            timestampOther = DateUtils.round(other.getLastModified(), Calendar.HOUR);
        } else if (frequency == MINUTE){
            timestamp = DateUtils.round(getLastModified(), Calendar.MINUTE);
            timestampOther = DateUtils.round(other.getLastModified(), Calendar.MINUTE);
        } else {
            timestamp = DateUtils.round(getLastModified(), Calendar.SECOND);
            timestampOther = DateUtils.round(other.getLastModified(), Calendar.SECOND);
        }
        return timestamp.equals(timestampOther);
    }

    public String asPriceVolumeString() {
        return millisFormat.format(getLastModified()) + "," + getBestBidSize() + "," + getBestBidPrice() + "," + getBestAskPrice() + "," + getBestAskSize();
    }

    public static OrderBookLevelOneImmutable fromJSON(JsonObject json) throws ParseException {
        Date lastModified = isoFormat.parse(json.getString("lastModified"));
        BigDecimal bestBidPrice = BigDecimal.valueOf(json.getDouble("bestBidPrice"));
        BigDecimal bestAskPrice = BigDecimal.valueOf(json.getDouble("bestAskPrice"));
        Integer bestBidSize = json.getInteger("bestBidSize");
        Integer bestAskSize = json.getInteger("bestAskSize");
        return new OrderBookLevelOneImmutable(lastModified, bestBidSize, bestBidPrice, bestAskPrice, bestAskSize);
    }
}
