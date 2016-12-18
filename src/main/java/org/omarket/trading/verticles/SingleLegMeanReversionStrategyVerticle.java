package org.omarket.trading.verticles;

import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.omarket.trading.quote.Quote;

import java.math.BigDecimal;
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
        logger.info("using default paramater for thresholdStep");
        getParameters().put("thresholdStep", 0.1);
    }

    /**
     * @param quotes current quotes for each product
     */
    @Override
    public void processQuotes(Map<String, Quote> quotes) {
        Quote quote = quotes.get(IB_CODE_EUR_CHF);
        if(quote != null) {
            BigDecimal midPrice = quote.getBestBidPrice().add(quote.getBestAskPrice()).divide(BigDecimal.valueOf(2));
            JsonObject message = new JsonObject();
            message.put("signal", midPrice.doubleValue());
            message.put("thresholdLow1", (1 - 3 * getParameters().getDouble("thresholdStep")) * midPrice.doubleValue());
            logger.debug("emitting: " + message + " (timestamp: " + quote.getLastModified() + ")");
            vertx.eventBus().send(ADDRESS_STRATEGY_SIGNAL, message);
        }
    }

}
