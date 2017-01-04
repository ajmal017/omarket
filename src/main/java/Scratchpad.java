
import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.core.Vertx;
import joinery.DataFrame;
import org.apache.commons.math3.exception.DimensionMismatchException;
import org.apache.commons.math3.exception.NullArgumentException;
import org.apache.commons.math3.filter.MeasurementModel;
import org.apache.commons.math3.filter.ProcessModel;
import org.apache.commons.math3.linear.*;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.commons.math3.util.Pair;
import org.omarket.stats.KalmanFilter;
import org.omarket.trading.quote.Quote;
import org.omarket.trading.quote.QuoteConverter;
import org.omarket.trading.verticles.HistoricalDataVerticle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.functions.Func1;
import rx.Observable;
import rx.schedulers.Schedulers;
import yahoofinance.Stock;
import yahoofinance.YahooFinance;
import yahoofinance.histquotes.HistoricalQuote;
import yahoofinance.histquotes.Interval;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static java.lang.Thread.sleep;
import static org.omarket.trading.verticles.VerticleProperties.PROPERTY_IBROKERS_TICKS_PATH;
import static org.omarket.trading.verticles.HistoricalDataVerticle.mergeQuoteStreams;
import static org.omarket.trading.verticles.HistoricalDataVerticle.getHistoricalQuoteStream;
import static rx.Observable.combineLatest;
import static rx.Observable.interval;
import static yahoofinance.Utils.storeObject;

class RandomWalk implements Func1<Boolean, Double> {
    private final Double step;
    private Double state = 0.;

    RandomWalk(Double initial, Double step) {
        this.step = step;
        this.state = initial;
    }

    @Override
    public Double call(Boolean upOrDown) {
        state = upOrDown ? state + step : state - step;
        return state;
    }

}

class Hysteresis implements Func1<Double, Double> {
    private final Double outputLow;
    private final Double outputHigh;
    private final Double thresholdLow;
    private final Double thresholdHigh;
    private Double state;

    Hysteresis(Double outputLow, Double outputHigh, Double thresholdLow, Double thresholdHigh) {
        this.outputLow = outputLow;
        this.outputHigh = outputHigh;
        this.thresholdLow = thresholdLow;
        this.thresholdHigh = thresholdHigh;
        state = outputLow;
    }

    @Override
    public Double call(Double value) {
        if (value >= thresholdHigh) {
            state = outputHigh;
        } else if (value <= thresholdLow) {
            state = outputLow;
        }
        return state;
    }
}

public class Scratchpad {
    private final static Logger logger = LoggerFactory.getLogger(Scratchpad.class);

    public static void main0(String[] args) throws InterruptedException {
        Observable<Long> clock = interval(100, TimeUnit.MILLISECONDS, Schedulers.computation());

        final Random random = new Random();

        Double mean = 100.;
        Double step = 0.1;
        Observable<Double> randomWalk1 = clock
                .map(tick -> random.nextBoolean())
                .map(new RandomWalk(mean, step));

        Observable<Double> randomWalk2 = clock.buffer(1)
                .map(tick -> random.nextBoolean())
                .map(new RandomWalk(mean, step));

        Observable<Double> signal = combineLatest(randomWalk1, randomWalk2, (value1, value2) -> value2 - value1);
        Observable<Double> filtered = signal.map(new Hysteresis(-1., 1., -1., 1.));

        signal.subscribe(value -> {
            logger.info("raw value = " + value);
        });

        filtered.subscribe(value -> {
            logger.info("filtered value = " + value);
        });
        sleep(10000);
    }

    public static void saveYahooExamples() throws Exception {
        String[] symbols = new String[]{"EWC", "EWA", "USO"};
        Calendar from = Calendar.getInstance();
        from.add(Calendar.YEAR, -5);
        Map<String, Stock> stocks = YahooFinance.get(symbols, from, Interval.DAILY);
        Stock ewc = stocks.get("EWC");
        Stock ewa = stocks.get("EWA");
        Stock uso = stocks.get("USO");
        List<HistoricalQuote> historyEWC = ewc.getHistory();
        List<HistoricalQuote> historyEWA = ewa.getHistory();
        List<HistoricalQuote> historyUSO = uso.getHistory();
        storeObject(Paths.get("data", "ewc-hist.bin"), historyEWC);
        storeObject(Paths.get("data", "ewa-hist.bin"), historyEWA);
        storeObject(Paths.get("data", "uso-hist.bin"), historyUSO);
    }

    public static void main(String[] args) throws Exception {
        Path pricesPath = Paths.get("python-lab", "prices.csv");
        DataFrame df = DataFrame.readCsv(Files.newInputStream(pricesPath));
        logger.info("df:\n" + df.toString(5000));

        List<Pair<Double, Double>> values = new LinkedList<>();
        SimpleRegression regression = new SimpleRegression(true);
        for (ListIterator it = df.iterrows(); it.hasNext(); ) {
            List row = (List) it.next();
            Double ewaValue = (Double) row.get(1);
            Double ewcValue = (Double) row.get(2);
            values.add(new Pair<>(ewaValue, ewcValue));
            logger.info("adding value for " + row.get(0));
            regression.addData(ewaValue, ewcValue);
        }
        modifiedKalman(values);
        logger.info("data size: " + values.size());
        logger.info("regression R2: " + regression.getRSquare());
        logger.info("regression data: " + regression.getSlope() + ", " + regression.getIntercept());
        // const    5.315368
        // slope    0.988800
    }

    /**
     * @param pairs List of (input value, output value) pairs
     */
    private static void modifiedKalman(List<Pair<Double, Double>> pairs) {
        ProcessModel pm = new RegressionProcessModel();
        MeasurementModel mm = new RegressionMeasurementModel();
        ModifiedKalmanFilter kf = new ModifiedKalmanFilter(pm, mm);
        for (Pair<Double, Double> pair : pairs) {
            // prediction phase
            kf.predict();

            // measurement update
            RealMatrix measurementMatrix = MatrixUtils.createRowRealMatrix(new double[]{pair.getFirst(), 1.});
            kf.updateMeasurementMatrix(measurementMatrix);

            // correction phase
            RealVector z = MatrixUtils.createRealVector(new double[]{pair.getSecond()});
            logger.debug("values=" + pair.getFirst() + " - " + z);
            kf.correct(z);
        }
        logger.info("prediction slope / intercept:" + kf.getStateEstimationVector());
        logger.info("error estimate: " + kf.getErrorCovarianceMatrix());

    }

    static class ModifiedKalmanFilter extends KalmanFilter {
        /**
         * Creates a new Kalman filter with the given process and measurement models.
         *
         * @param process     the model defining the underlying process dynamics
         * @param measurement the model defining the given measurement characteristics
         * @throws NullArgumentException            if any of the given inputs is null (except for the control matrix)
         * @throws NonSquareMatrixException         if the transition matrix is non square
         * @throws DimensionMismatchException       if the column dimension of the transition matrix does not match the dimension of the
         *                                          initial state estimation vector
         * @throws MatrixDimensionMismatchException if the matrix dimensions do not fit together
         */
        public ModifiedKalmanFilter(ProcessModel process, MeasurementModel measurement) throws NullArgumentException, NonSquareMatrixException, DimensionMismatchException, MatrixDimensionMismatchException {
            super(process, measurement);
        }

        public void updateMeasurementMatrix(RealMatrix newMeasurementMatrix) {
            this.measurementMatrix = newMeasurementMatrix;
            this.measurementMatrixT = newMeasurementMatrix.transpose();
        }
    }

    public static void main1(String[] args) throws Exception {
        JsonArray defaultStoragePath = new JsonArray(Arrays.asList("data", "ticks"));
        int defaultClientId = 1;
        String defaultHost = "127.0.0.1";
        int defaultPort = 7497;
        JsonObject jsonConfig = new JsonObject()
                .put(PROPERTY_IBROKERS_TICKS_PATH, defaultStoragePath)
                .put("ibrokers.clientId", defaultClientId)
                .put("ibrokers.host", defaultHost)
                .put("ibrokers.port", defaultPort)
                .put("runBacktestFlag", false);
        DeploymentOptions options = new DeploymentOptions().setConfig(jsonConfig);

        final Vertx vertx = Vertx.vertx();

        JsonArray storageDirs = new JsonArray();
        storageDirs.add("data");
        storageDirs.add("ticks");
        List<String> dirs = storageDirs.getList();
        Observable<Quote> stream1 = getHistoricalQuoteStream(dirs, "12087817");
        Observable<Quote> stream2 = getHistoricalQuoteStream(dirs, "12087820");
        List<Observable<Quote>> quoteStreams = Arrays.asList(stream1, stream2);
        mergeQuoteStreams(quoteStreams)
                .map(QuoteConverter::toJSON)
                .forEach(
                        quoteJson -> {
                            logger.info("sending: " + quoteJson);
                            vertx.eventBus().send(HistoricalDataVerticle.ADDRESS_PROVIDE_HISTORY, quoteJson);
                        }
                );
    }

    private static class RegressionProcessModel implements ProcessModel {
        @Override
        public RealMatrix getStateTransitionMatrix() {
            return MatrixUtils.createRealIdentityMatrix(2);
        }

        @Override
        public RealMatrix getControlMatrix() {
            return null;
        }

        @Override
        public RealMatrix getProcessNoise() {
            double delta = 1E-5;
            return MatrixUtils.createRealIdentityMatrix(2).scalarMultiply(delta / (1. - delta));
        }

        @Override
        public RealVector getInitialStateEstimate() {
            return MatrixUtils.createRealVector(new double[]{0., 0.});
        }

        @Override
        public RealMatrix getInitialErrorCovariance() {
            //return MatrixUtils.createRealMatrix(new double[][]{{1., 1.}, {1., 1.}});
            return MatrixUtils.createRealIdentityMatrix(2);
        }
    }

    private static class RegressionMeasurementModel implements MeasurementModel {

        @Override
        public RealMatrix getMeasurementMatrix() {
            return MatrixUtils.createRowRealMatrix(new double[]{0., 1.});
        }

        @Override
        public RealMatrix getMeasurementNoise() {
            return MatrixUtils.createRealMatrix(new double[][]{{1E-3}});
        }
    }
}
