package org.omarket.trading.quote;

import io.vertx.core.json.JsonObject;
import sun.util.resources.cldr.af.LocaleNames_af;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
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
    private final static DateTimeFormatter isoFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss:SSS'Z'");

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
        LocalDateTime lastModifiedLocal = LocalDateTime.parse(json.getString("lastModified"), isoFormat);
        ZonedDateTime lastModified = ZonedDateTime.of(lastModifiedLocal, ZoneOffset.UTC);
        BigDecimal bestBidPrice = BigDecimal.valueOf(json.getDouble("bestBidPrice"));
        BigDecimal bestAskPrice = BigDecimal.valueOf(json.getDouble("bestAskPrice"));
        Integer bestBidSize = json.getInteger("bestBidSize");
        Integer bestAskSize = json.getInteger("bestAskSize");
        return QuoteFactory.create(lastModified, bestBidSize, bestBidPrice, bestAskPrice, bestAskSize);
    }
}
