package org.omarket.trading.verticles;

import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import joinery.DataFrame;
import org.apache.commons.math3.filter.KalmanFilter;
import org.apache.commons.math3.filter.MeasurementModel;
import org.apache.commons.math3.filter.ProcessModel;
import org.apache.commons.math3.linear.*;
import org.omarket.trading.quote.Quote;

import java.math.BigDecimal;
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
        return new String[]{IB_CODE_GCG7, IB_CODE_GDX_ARCA};
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
        if (sampledQuotes.get(IB_CODE_GCG7) == null || sampledQuotes.get(IB_CODE_GDX_ARCA) == null) {
            return;
        }
        DataFrame samplesGold = sampledQuotes.get(IB_CODE_GCG7);
        DataFrame samplesGDX = sampledQuotes.get(IB_CODE_GDX_ARCA);
        samplesGold.applyRows("mid", row -> {
            BigDecimal bid = (BigDecimal) row.get("bid");
            BigDecimal ask = (BigDecimal) row.get("ask");
            return 0.5 * (bid.doubleValue() + ask.doubleValue());
        });
        samplesGDX.applyRows("mid", row -> {
            BigDecimal bid = (BigDecimal) row.get("bid");
            BigDecimal ask = (BigDecimal) row.get("ask");
            return 0.5 * (bid.doubleValue() + ask.doubleValue());
        });
        logger.info("gold:\n" + samplesGold);
        logger.info("gdx:\n" + samplesGDX);
        Double[] midGoldValues = (Double[]) samplesGold.col("mid").toArray(new Double[0]);
        Double[] midGDXValues = (Double[]) samplesGDX.col("mid").toArray(new Double[0]);
        logger.info("gold values:" + samplesGold.col("mid"));
        logger.info("gdx values:" + samplesGDX.col("mid"));
        ProcessModel pm = new LinearRegressionProcessModel();
        LinearRegressionMeasurementModel mm = new LinearRegressionMeasurementModel();
        KalmanFilter filter = new KalmanFilter(pm, mm);
        // have a look at: http://www.bzarg.com/p/how-a-kalman-filter-works-in-pictures/
        int count = 0;
        while (count < midGDXValues.length){
            double independentVariable = midGDXValues[count];
            double dependentVariable = midGoldValues[count];
            filter.predict();
            mm.setMeasurement(independentVariable);
            filter.correct(new double[]{dependentVariable});
            count++;
            logger.info("estimates: " + Arrays.asList(filter.getStateEstimation()));
        }

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

    private static class LinearRegressionProcessModel implements ProcessModel {
        /**
         * Our model assumes the hidden state is a random walk, so the state transition
         * matrix is the identity.
         *
         * @return Identity matrix
         */
        @Override
        public RealMatrix getStateTransitionMatrix() {
            return new DiagonalMatrix(new double[]{1., 1.});
        }

        /**
         * @return our model assumes no external control
         */
        @Override
        public RealMatrix getControlMatrix() {
            return new DiagonalMatrix(new double[]{0., 0.});
        }

        /**
         * @return process noise matrix
         */
        @Override
        public RealMatrix getProcessNoise() {
            // TODO
            return null;
        }

        @Override
        public RealVector getInitialStateEstimate() {
            return new ArrayRealVector(new double[]{0., 0.});
        }

        @Override
        public RealMatrix getInitialErrorCovariance() {
            return new DiagonalMatrix(new double[]{0., 0.});
        }
    }

    private static class LinearRegressionMeasurementModel implements MeasurementModel {
        private Double currentMeasurement = 0.;
        /**
         * This is the observation model, that is the second asset price
         * augmented with 1. in the second column.
         *
         * @return observation model matrix
         */
        @Override
        public RealMatrix getMeasurementMatrix() {
            return new Array2DRowRealMatrix(new double[]{currentMeasurement, 1.}).transpose();
        }

        /**
         * @return measurement noise matrix
         */
        @Override
        public RealMatrix getMeasurementNoise() {
            // TODO
            return null;
        }

        void setMeasurement(double value){
            currentMeasurement = value;
        }
    }
}