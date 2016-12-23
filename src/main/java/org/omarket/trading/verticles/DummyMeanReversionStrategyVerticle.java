package org.omarket.trading.verticles;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.omarket.trading.quote.Quote;
import rx.Observable;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.temporal.ChronoUnit;
import java.util.Deque;
import java.util.Map;

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
     * @param quoteRecordsByProduct quotes history for each product
     */
    @Override
    public void processQuotes(Map<String, Quote> quoteRecordsByProduct, Map<String, Deque<Quote>> quotes, Map<String, Deque<Quote>> sampledQuotes) {
        logger.info("processing: " + quoteRecordsByProduct);
        logger.info("processing samples: " + sampledQuotes);
        if (sampledQuotes.get(IB_CODE_USD_CHF) == null) {
            return;
        }
        Deque<Quote> samples = sampledQuotes.get(IB_CODE_USD_CHF);
        for(Quote sample: samples){
            logger.info("available sample: " + sample);
        }
        Observable<Quote> quotesStream = Observable.from(sampledQuotes.get(IB_CODE_USD_CHF));
        Observable<BigDecimal> askStream = quotesStream.map(Quote::getBestAskPrice);
        Observable<BigDecimal> bidStream = quotesStream.map(Quote::getBestBidPrice);
        Observable<BigDecimal> midStream = askStream
                .zipWith(bidStream, (x, y) -> x.add(y).divide(BigDecimal.valueOf(2), RoundingMode.HALF_UP));
        Observable<BigDecimal> delayedMidStream = midStream.buffer(2, 1).filter(x -> x.size() >= 2).map(x -> x.get(1));
        midStream.zipWith(delayedMidStream, (x1, x0) -> (BigDecimal.ONE.subtract(x1.divide(x0, RoundingMode.HALF_UP))))
                .doOnNext(x -> {
                    logger.info("value: " + x.multiply(BigDecimal.valueOf(10000)) + " bps");
                })
               // .subscribe()
        ;
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
