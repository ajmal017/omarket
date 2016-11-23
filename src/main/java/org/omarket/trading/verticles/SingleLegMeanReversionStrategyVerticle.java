package org.omarket.trading.verticles;

import com.jimmoores.quandl.DataSetRequest;
import com.jimmoores.quandl.QuandlSession;
import com.jimmoores.quandl.Row;
import com.jimmoores.quandl.TabularResult;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import joinery.DataFrame;
import org.omarket.trading.OrderBookLevelOneImmutable;
import org.threeten.bp.LocalDate;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static java.lang.Math.sqrt;

/**
 * Created by Christophe on 01/11/2016.
 */
public class SingleLegMeanReversionStrategyVerticle extends AbstractStrategyVerticle {
    private final static Logger logger = LoggerFactory.getLogger(SingleLegMeanReversionStrategyVerticle.class);
    final static String ADDRESS_STRATEGY_SIGNAL = "oot.strategy.signal.singleLeg";
    private final static Integer IB_CODE = 12087817;

    private static DataFrame<Double> loadQuandlInstrument(String quandlCode, int samples) {
        QuandlSession session = QuandlSession.create();
        DataSetRequest.Builder requestBuilder = DataSetRequest.Builder.of(quandlCode).withMaxRows(samples);
        TabularResult tabularResult = session.getDataSet(requestBuilder.build());
        Collection<String> columnNames = tabularResult.getHeaderDefinition().getColumnNames();
        DataFrame<Double> dataFrame = new DataFrame<>(columnNames);
        for(Row row: tabularResult){
            LocalDate date = row.getLocalDate("Date");
            Double value = row.getDouble("Value");
            Calendar calendar = new GregorianCalendar(date.getYear(), date.getMonthValue() + 1, date.getDayOfMonth());
            dataFrame.append(calendar.getTime(), Arrays.asList(new Double[]{value}));
        }
        return dataFrame;
    }

    @Override
    protected Integer[] getIBrokersCodes(){
        return new Integer[]{IB_CODE};
    }

    @Override
    protected void init(){
        logger.info("starting single leg mean reversion strategy verticle");
        DataFrame<Double> eurchfDaily = loadQuandlInstrument("ECB/EURCHF", 200);
        Double threasholdStep = eurchfDaily.percentChange().stddev().get(0, 1)/ sqrt(24*60*60);
        getParameters().put("thresholdStep", threasholdStep);
    }

    /**
     * @param orderBook
     * @param isBacktest
     */
    @Override
    protected void processOrderBook(OrderBookLevelOneImmutable orderBook, boolean isBacktest) {
        if(isBacktest){
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                logger.error("interrupted timer", e);
            }
        }
        BigDecimal midPrice = orderBook.getBestBidPrice().add(orderBook.getBestAskPrice()).divide(BigDecimal.valueOf(2));
        JsonObject message = new JsonObject();
        message.put("signal", midPrice.doubleValue());
        message.put("thresholdLow1", (1 - 3 * getParameters().getDouble("thresholdStep")) * midPrice.doubleValue());
        logger.info("emitting: " + message);
        vertx.eventBus().send(ADDRESS_STRATEGY_SIGNAL, message);
    }
}
