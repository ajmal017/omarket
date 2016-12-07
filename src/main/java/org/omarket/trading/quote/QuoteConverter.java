package org.omarket.trading.quote;

import io.vertx.core.json.JsonObject;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Created by Christophe on 07/12/2016.
 */
public class QuoteConverter {

    private final static SimpleDateFormat millisFormat;
    static {
        millisFormat = new SimpleDateFormat("mm:ss.SSS");
        millisFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    }
    protected final static SimpleDateFormat isoFormat;
    static {
        isoFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss:SSS'Z'");
        isoFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    }
    public static String toPriceVolumeString(Quote quote) {
        return millisFormat.format(quote.getLastModified()) + "," + quote.getBestBidSize() + "," + quote.getBestBidPrice() + "," + quote.getBestAskPrice() + "," + quote.getBestAskSize();
    }

    public static JsonObject toJSON(Quote quote) {
        JsonObject asJSON = new JsonObject();
        BigDecimal bidPrice = quote.getBestBidPrice();
        BigDecimal askPrice = quote.getBestAskPrice();
        asJSON.put("lastModified", isoFormat.format(quote.getLastModified()));
        asJSON.put("bestBidSize", quote.getBestBidSize());
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
        asJSON.put("bestAskSize", quote.getBestAskSize());
        return asJSON;
    }

    public static QuoteImpl fromJSON(JsonObject json) throws ParseException {
        Date lastModified = isoFormat.parse(json.getString("lastModified"));
        BigDecimal bestBidPrice = BigDecimal.valueOf(json.getDouble("bestBidPrice"));
        BigDecimal bestAskPrice = BigDecimal.valueOf(json.getDouble("bestAskPrice"));
        Integer bestBidSize = json.getInteger("bestBidSize");
        Integer bestAskSize = json.getInteger("bestAskSize");
        return QuoteFactory.create(lastModified, bestBidSize, bestBidPrice, bestAskPrice, bestAskSize);
    }
}
