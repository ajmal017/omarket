package org.omarket.trading;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Date;

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

    public OrderBookLevelOne(double minTick){
        String[] parts = Double.toString(minTick).split("\\.");
        if(parts[0].equals("0")) {
            decimalPrecision = parts[parts.length - 1].length();
        } else {
            decimalPrecision = 0;
        }
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

    public JsonObject asJSON(){
        Gson gson = new GsonBuilder().create();
        JsonObject asJSON = new JsonObject(gson.toJson(this));
        return asJSON;
    }
}
