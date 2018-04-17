package org.omarket.quotes;

import com.google.gson.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Created by Christophe on 07/12/2016.
 */
@Slf4j
@Component
public class QuoteConverter {

    private final static DateTimeFormatter millisFormat = DateTimeFormatter.ofPattern("mm:ss.SSS");
    private final static DateTimeFormatter isoFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss:SSS'Z'");

    private final QuoteFactory quoteFactory;

    @Autowired
    public QuoteConverter(QuoteFactory quoteFactory) {
        this.quoteFactory = quoteFactory;
    }

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
        asJSON.addProperty("lastModified", isoFormat.format(quote.getLastModified()));
        asJSON.addProperty("bestBidSize", quote.getBestBidSize());
        if (bidPrice != null) {
            asJSON.addProperty("bestBidPrice", bidPrice.doubleValue());
        } else {
            asJSON.addProperty("bestBidPrice", (Number) null);
        }
        if (askPrice != null) {
            asJSON.addProperty("bestAskPrice", askPrice.doubleValue());
        } else {
            asJSON.addProperty("bestAskPrice", (Number) null);
        }
        asJSON.addProperty("bestAskSize", quote.getBestAskSize());
        asJSON.addProperty("productCode", quote.getProductCode());
        return asJSON;
    }

    public Quote fromJSON(JsonObject json) throws ParseException {
        LocalDateTime lastModifiedLocal = LocalDateTime.parse(json.getAsJsonPrimitive("lastModified").getAsString(), isoFormat);
        ZonedDateTime lastModified = ZonedDateTime.of(lastModifiedLocal, ZoneOffset.UTC);
        BigDecimal bestBidPrice = BigDecimal.valueOf(json.getAsJsonPrimitive("bestBidPrice").getAsDouble());
        BigDecimal bestAskPrice = BigDecimal.valueOf(json.getAsJsonPrimitive("bestAskPrice").getAsDouble());
        Integer bestBidSize = json.getAsJsonPrimitive("bestBidSize").getAsInt();
        Integer bestAskSize = json.getAsJsonPrimitive("bestAskSize").getAsInt();
        String productCode = json.getAsJsonPrimitive("productCode").getAsString();
        return quoteFactory.create(lastModified, bestBidSize, bestBidPrice, bestAskPrice, bestAskSize, productCode);
    }
}
