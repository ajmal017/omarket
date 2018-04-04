package org.omarket.trading.quote;

import io.vertx.core.json.JsonObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Created by Christophe on 07/12/2016.
 */
@Component
public class QuoteConverter {

    private final static DateTimeFormatter millisFormat = DateTimeFormatter.ofPattern("mm:ss.SSS");
    private final static DateTimeFormatter isoFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss:SSS'Z'");

    @Autowired
    private QuoteFactory quoteFactory;

    public static String toPriceVolumeString(Quote quote) {
        String timestamp = millisFormat.format(quote.getLastModified());
        BigDecimal bidPrice = quote.getBestBidPrice();
        BigDecimal askPrice = quote.getBestAskPrice();
        return timestamp + "," + quote.getBestBidSize() + "," + bidPrice + "," + askPrice + "," + quote.getBestAskSize();
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
        asJSON.put("productCode", quote.getProductCode());
        return asJSON;
    }

    public Quote fromJSON(JsonObject json) throws ParseException {
        LocalDateTime lastModifiedLocal = LocalDateTime.parse(json.getString("lastModified"), isoFormat);
        ZonedDateTime lastModified = ZonedDateTime.of(lastModifiedLocal, ZoneOffset.UTC);
        BigDecimal bestBidPrice = BigDecimal.valueOf(json.getDouble("bestBidPrice"));
        BigDecimal bestAskPrice = BigDecimal.valueOf(json.getDouble("bestAskPrice"));
        Integer bestBidSize = json.getInteger("bestBidSize");
        Integer bestAskSize = json.getInteger("bestAskSize");
        String productCode = json.getString("productCode");
        return quoteFactory.create(lastModified, bestBidSize, bestBidPrice, bestAskPrice, bestAskSize, productCode);
    }
}
