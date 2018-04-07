package org.omarket.trading.verticles;

import joinery.DataFrame;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.filter.KalmanFilter;
import org.apache.commons.math3.filter.MeasurementModel;
import org.apache.commons.math3.filter.ProcessModel;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.DiagonalMatrix;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.omarket.trading.Security;
import org.omarket.trading.quote.Quote;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Deque;
import java.util.Map;

/**
 * Created by Christophe on 01/11/2016.
 */
@Slf4j
@Component
public class DummyMeanReversionStrategyVerticle extends AbstractStrategyVerticle {
    private final static String IB_CODE_GCG7 = "188989072";
    private final static String IB_CODE_GDX_ARCA = "229726316";
    private final static String IB_CODE_EUR_CHF = "12087817";
    private final static String IB_CODE_USD_CHF = "12087820";
    private final static String IB_CODE_EUR_SEK = "37893488";
    @Value("${address.strategy_signal}")
    private String ADDRESS_STRATEGY_SIGNAL;

    @Override
    protected String[] getProductCodes() {
        return new String[]{IB_CODE_GCG7, IB_CODE_GDX_ARCA};
    }

    @Override
    protected void init() {
        log.info("starting single leg mean reversion strategy verticle");
        log.info("using default parameter for thresholdStep");
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
    public void processQuotes(Map<String, Security> contracts, Map<String, Deque<Quote>> quotes, Map<String, DataFrame> sampledQuotes) {
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
        log.info("gold:\n" + samplesGold);
        log.info("gdx:\n" + samplesGDX);
        Double[] midGoldValues = (Double[]) samplesGold.col("mid").toArray(new Double[0]);
        Double[] midGDXValues = (Double[]) samplesGDX.col("mid").toArray(new Double[0]);
        log.info("gold values:" + samplesGold.col("mid"));
        log.info("gdx values:" + samplesGDX.col("mid"));
        ProcessModel pm = new LinearRegressionProcessModel();
        LinearRegressionMeasurementModel mm = new LinearRegressionMeasurementModel();
        KalmanFilter filter = new KalmanFilter(pm, mm);
        // have a look at: http://www.bzarg.com/p/how-a-kalman-filter-works-in-pictures/
        int count = 0;
        while (count < midGDXValues.length) {
            double independentVariable = midGDXValues[count];
            double dependentVariable = midGoldValues[count];
            filter.predict();
            mm.setMeasurement(independentVariable);
            filter.correct(new double[]{dependentVariable});
            count++;
            log.info("estimates: " + Arrays.asList(filter.getStateEstimation()));
        }

        log.info("*** completed processing quote ***");
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
            return new DiagonalMatrix(new double[]{1., 1.});
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
            return MatrixUtils.createRowRealMatrix(new double[]{1., 1.});
        }

        void setMeasurement(double value) {
            currentMeasurement = value;
        }
    }
}
