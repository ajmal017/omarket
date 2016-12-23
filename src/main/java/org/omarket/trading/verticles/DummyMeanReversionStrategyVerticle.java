package org.omarket.trading.verticles;

import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import joinery.DataFrame;
import org.omarket.trading.quote.Quote;

import java.time.temporal.ChronoUnit;
import java.util.*;

/**
 * Created by Christophe on 01/11/2016.
 */
public class DummyMeanReversionStrategyVerticle extends AbstractStrategyVerticle {
    private final static Logger logger = LoggerFactory.getLogger(DummyMeanReversionStrategyVerticle.class);
    final static String ADDRESS_STRATEGY_SIGNAL = "oot.strategy.signal.dummy";

    private final static String IB_CODE_GCG7 = "188989072";
    private final static String IB_CODE_GDX_ARCA = "229726316";

    private final static String IB_CODE_EUR_CHF = "12087817";
    private final static String IB_CODE_USD_CHF = "12087820";
    private final static String IB_CODE_EUR_SEK = "37893488";

    @Override
    protected String[] getProductCodes() {
        return new String[]{IB_CODE_USD_CHF};
    }

    @Override
    protected void init() {
        logger.info("starting single leg mean reversion strategy verticle");
        logger.info("using default parameter for thresholdStep");
        getParameters().put("thresholdStep", 0.1);
    }

    @Override
    protected Integer getSampledDataSize() {
        return 1000;
    }

    @Override
    protected ChronoUnit getSampleDataUnit() {
        return ChronoUnit.SECONDS;
    }

    /**
     * @param contracts     mapping each contract code to the relevant contract properties
     * @param quotes        tick data, in increasing order of timestamp (last is most recent)
     * @param sampledQuotes sampled data
     */
    @Override
    public void processQuotes(Map<String, JsonObject> contracts, Map<String, Deque<Quote>> quotes, Map<String, DataFrame> sampledQuotes) {
        if (sampledQuotes.get(IB_CODE_USD_CHF) == null) {
            return;
        }
        logger.info("contract: " + contracts.get(IB_CODE_USD_CHF));
        DataFrame samples = sampledQuotes.get(IB_CODE_USD_CHF);
        logger.info("first sample: " + samples.head(1));
        logger.info("last sample: " + samples.tail(1));
        logger.info("dataframe: \n" + samples);

        /*
        for(String productCode: sampledQuotes.keySet()){
            Deque<Quote> productQuotes = sampledQuotes.get(productCode);
            int length = 0;
            if (productQuotes != null){
                length = productQuotes.size();
            }
            logger.info("length of sampled quotes history for " + productCode + ": " + length);
        }
        for(String productCode: sampledQuotes.keySet()){
            Deque<Quote> productQuotes = sampledQuotes.get(productCode);
            String range = null;
            if (productQuotes != null){
                range = "[" + productQuotes.getFirst().getLastModified() + ", " + productQuotes.getLast().getLastModified() + "]";
            }
            logger.info("range samples: " + range);
        }
        for(String productCode: quotes.keySet()){
            Deque<Quote> productQuotes = quotes.get(productCode);
            int length = 0;
            if (productQuotes != null){
                length = productQuotes.size();
            }
            logger.info("length of quotes history for " + productCode + ": " + length);
        }
        */
        /*
        for(String productCode: sampledQuotes.keySet()){
            if (!productCode.equals(IB_CODE_EUR_CHF)) {
                continue;
            }
            Deque<Quote> samples = sampledQuotes.get(productCode);
            for(Quote sample: samples){
                logger.info("available sample: " + sample.getLastModified());
            }
            Quote quote = quoteRecordsByProduct.get(productCode);
            BigDecimal midPrice = quote.getBestBidPrice().add(quote.getBestAskPrice()).divide(BigDecimal.valueOf(2));
            JsonObject message = new JsonObject();
            message.put("signal", midPrice.doubleValue());
            message.put("thresholdLow1", (1 - 3 * getParameters().getDouble("thresholdStep")) * midPrice.doubleValue());
            logger.debug("emitting: " + message + " (timestamp: " + quote.getLastModified() + ")");
            vertx.eventBus().send(ADDRESS_STRATEGY_SIGNAL, message);
        }
        */
        logger.info("*** completed processing quote ***");
    }

}
