package org.omarket.trading.verticles;

import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.omarket.trading.quote.Quote;
import rx.Observable;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * Created by Christophe on 01/11/2016.
 */
public class SingleLegMeanReversionStrategyVerticle extends AbstractStrategyVerticle {
    private final static Logger logger = LoggerFactory.getLogger(SingleLegMeanReversionStrategyVerticle.class);
    final static String ADDRESS_STRATEGY_SIGNAL = "oot.strategy.signal.singleLeg";
    private final static String IB_CODE_EUR_CHF = "12087817";
    private final static String IB_CODE_USD_CHF = "12087820";

    @Override
    protected String[] getProductCodes(){
        return new String[]{IB_CODE_EUR_CHF, IB_CODE_USD_CHF};
    }

    @Override
    protected void init(){
        logger.info("starting single leg mean reversion strategy verticle");
        logger.info("using default parameter for thresholdStep");
        getParameters().put("thresholdStep", 0.1);
    }

    @Override
    protected Integer getTickHistorySize() {
        return 10;
    }

    /**
     * @param quoteRecordsByProduct quotes history for each product
     */
    @Override
    public void processQuotes(Map<String, List<Quote>> quoteRecordsByProduct) {
        List<Quote> quoteRecords = quoteRecordsByProduct.get(IB_CODE_EUR_CHF);
        if(quoteRecords == null){
            return;
        }
        Observable<Quote> quotesStream = Observable.from(quoteRecords);
        Observable<BigDecimal> askStream = quotesStream.map(Quote::getBestAskPrice);
        Observable<BigDecimal> bidStream = quotesStream.map(Quote::getBestBidPrice);
        Observable<BigDecimal> midStream = askStream.zipWith(bidStream, (x, y) -> x.add(y).divide(BigDecimal.valueOf(2)));
        Observable<BigDecimal> delayedMidStream = midStream.buffer(2, 1).map(x -> x.get(1));


        int count = 1;
        for(String product: quoteRecordsByProduct.keySet()){
            List<Quote> currentRecords = quoteRecordsByProduct.get(product);
            logger.info("records length for product " + count + " (" + product + "): " + currentRecords.size());
            String fromThrough = "" + currentRecords.get(0) + " -> " + currentRecords.get(currentRecords.size() - 1);
            logger.info("records: " + fromThrough);
            count += 1;
        }
        if(quoteRecords != null) {
            Quote quote = quoteRecords.get(quoteRecords.size() - 1);
            BigDecimal midPrice = quote.getBestBidPrice().add(quote.getBestAskPrice()).divide(BigDecimal.valueOf(2));
            JsonObject message = new JsonObject();
            message.put("signal", midPrice.doubleValue());
            message.put("thresholdLow1", (1 - 3 * getParameters().getDouble("thresholdStep")) * midPrice.doubleValue());
            logger.debug("emitting: " + message + " (timestamp: " + quote.getLastModified() + ")");
            vertx.eventBus().send(ADDRESS_STRATEGY_SIGNAL, message);
        }
    }

}
