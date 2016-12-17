package org.omarket.trading.verticles;

import com.jimmoores.quandl.DataSetRequest;
import com.jimmoores.quandl.QuandlSession;
import com.jimmoores.quandl.Row;
import com.jimmoores.quandl.TabularResult;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import joinery.DataFrame;
import org.omarket.trading.quote.Quote;
import org.threeten.bp.LocalDate;

import java.math.BigDecimal;
import java.util.*;

import static java.lang.Math.sqrt;

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
    protected Integer getLookBackPeriod() {
        return 200 * 1000;
    }

    @Override
    protected void init(Integer lookBackPeriod){
        logger.info("starting single leg mean reversion strategy verticle");
        logger.info("using default paramater for thresholdStep");
        getParameters().put("thresholdStep", 0.1);
    }

    /**
     * @param quote
     */
    @Override
    public void processQuote(Quote quote) {
        BigDecimal midPrice = quote.getBestBidPrice().add(quote.getBestAskPrice()).divide(BigDecimal.valueOf(2));
        JsonObject message = new JsonObject();
        message.put("signal", midPrice.doubleValue());
        message.put("thresholdLow1", (1 - 3 * getParameters().getDouble("thresholdStep")) * midPrice.doubleValue());
        logger.info("emitting: " + message);
        List<Quote> pastQuotes = this.getPastQuotes();
        logger.info("length of past quotes: " + pastQuotes.size());
        vertx.eventBus().send(ADDRESS_STRATEGY_SIGNAL, message);
    }

}
