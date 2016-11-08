package org.omarket.trading;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.oracle.javafx.jmx.json.JSONDocument;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Created by Christophe on 04/11/2016.
 */
public class OrderBookLevelOne {

    private static Logger logger = LoggerFactory.getLogger(OrderBookLevelOne.class);

    private Date lastUpdate = null;
    private BigDecimal bestBidPrice = null;
    private BigDecimal bestAskPrice = null;
    private Integer bestBidSize = null;
    private Integer bestAskSize = null;
    private int decimalPrecision;
    private final SimpleDateFormat millisFormat;
    private final SimpleDateFormat isoFormat;

    public OrderBookLevelOne(double minTick) {
        String[] parts = Double.toString(minTick).split("\\.");
        if (parts[0].equals("0")) {
            decimalPrecision = parts[parts.length - 1].length();
        } else {
            decimalPrecision = 0;
        }
        millisFormat = new SimpleDateFormat("mm:ss.SSS");
        millisFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        isoFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss:SSS'Z'");
        isoFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    public void setBestBidSize(int size) {
        bestBidSize = size;
        setLastUpdate(new Date());
    }

    public void setBestAskSize(int size) {
        bestAskSize = size;
        setLastUpdate(new Date());
    }

    public void setBestBidPrice(double price) {
        bestBidPrice = BigDecimal.valueOf(price).setScale(decimalPrecision, BigDecimal.ROUND_HALF_UP);
        setLastUpdate(new Date());
    }

    public void setBestAskPrice(double price) {
        bestAskPrice = BigDecimal.valueOf(price).setScale(decimalPrecision, BigDecimal.ROUND_HALF_UP);
        setLastUpdate(new Date());
    }

    private void setLastUpdate(Date lastUpdate) {
        this.lastUpdate = lastUpdate;
    }

    public Date getLastUpdate() {
        return lastUpdate;
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

    public JsonObject asJSON() {
        JsonObject asJSON = new JsonObject();
        BigDecimal bidPrice = getBestBidPrice();
        BigDecimal askPrice = getBestAskPrice();
        asJSON.put("lastUpdate", isoFormat.format(getLastUpdate()));
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
        return this.millisFormat.format(getLastUpdate()) + "," + getBestBidSize() + "," + getBestBidPrice() + "," + getBestAskPrice() + "," + getBestAskSize();
    }
}
