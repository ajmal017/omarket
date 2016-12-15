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
    private final static Integer IB_CODE_EUR_CHF = 12087817;
    private final static Integer IB_CODE_USD_CHF = 12087820;

    private static DataFrame<Double> loadQuandlInstrument(String quandlCode, int samples) {
        DataFrame<Double> dataFrame = null;
        try {
            logger.info("accessing Quandl data");
            QuandlSession session = QuandlSession.create();
            DataSetRequest.Builder requestBuilder = DataSetRequest.Builder.of(quandlCode).withMaxRows(samples);
            TabularResult tabularResult = session.getDataSet(requestBuilder.build());
            Collection<String> columnNames = tabularResult.getHeaderDefinition().getColumnNames();
            dataFrame = new DataFrame<>(columnNames);
            for (Row row : tabularResult) {
                LocalDate date = row.getLocalDate("Date");
                Double value = row.getDouble("Value");
                Calendar calendar = new GregorianCalendar(date.getYear(), date.getMonthValue() + 1, date.getDayOfMonth());
                dataFrame.append(calendar.getTime(), Arrays.asList(new Double[]{value}));
            }
        } catch(javax.ws.rs.ProcessingException e){
            logger.error("unable to access Quandl");
        }
        return dataFrame;
    }

    @Override
    protected Integer[] getIBrokersCodes(){
        return new Integer[]{IB_CODE_EUR_CHF, IB_CODE_USD_CHF};
    }

    @Override
    protected Integer getLookBackPeriod() {
        return 200 * 1000;
    }

    @Override
    protected void init(Integer lookBackPeriod){
        logger.info("starting single leg mean reversion strategy verticle");
        DataFrame<Double> eurchfDaily = loadQuandlInstrument("ECB/EURCHF", 200);
        if(eurchfDaily != null) {
            Double thresholdStep = eurchfDaily.percentChange().stddev().get(0, 1) / sqrt(24 * 60 * 60);
            getParameters().put("thresholdStep", thresholdStep);
        } else {
            logger.info("using default paramater for thresholdStep");
            getParameters().put("thresholdStep", 0.1);
        }
    }

    /**
     * @param quote
     * @param isBacktest
     */
    @Override
    public void processQuote(Quote quote, boolean isBacktest) {
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
