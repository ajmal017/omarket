
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Verticle;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.core.RxHelper;
import io.vertx.rxjava.core.Vertx;
import org.omarket.trading.quote.Quote;
import org.omarket.trading.quote.QuoteConverter;
import org.omarket.trading.verticles.HistoricalDataVerticle;
import org.omarket.trading.verticles.QuoteProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.functions.Func1;
import rx.Observable;
import rx.schedulers.Schedulers;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static java.lang.Thread.sleep;
import static org.omarket.trading.MarketData.IBROKERS_TICKS_STORAGE_PATH;
import static org.omarket.trading.verticles.HistoricalDataVerticle.processHistoricalQuotes;
import static rx.Observable.*;

class RandomWalk implements Func1<Boolean, Double> {
    private final static Logger logger = LoggerFactory.getLogger(RandomWalk.class);
    private final Double step;
    private Double state = 0.;

    RandomWalk(Double initial, Double step) {
        this.step = step;
        this.state = initial;
    }

    @Override
    public Double call(Boolean upOrDown) {
        if(upOrDown){
            logger.info("up one tick");
        } else {
            logger.info("down one tick");
        }
        state = upOrDown? state + step: state - step;
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
        if(value >= thresholdHigh){
            state = outputHigh;
        } else if (value <= thresholdLow){
            state = outputLow;
        }
        return state;
    }
}

public class Scratchpad {
    private final static Logger logger = LoggerFactory.getLogger(Scratchpad.class);

    public static void main2(String[] args) throws InterruptedException {
        DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        String value = "2016-11-14 10:53:59.260";
        System.out.println(LocalDateTime.parse(value, DATE_FORMAT));
    }

    public static void main3(String[] args) throws InterruptedException {
        Observable<Long> clock = interval(100, TimeUnit.MILLISECONDS, Schedulers.computation());

        final Random random = new Random();

        Double mean = 100.;
        Double step = 0.1;
        Observable<Double> randomWalk1 = clock
                .map(tick -> random.nextBoolean())
                .map(new RandomWalk(mean, step));

        Observable<Double> randomWalk2 = clock
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

    public static void main(String[] args) throws Exception {

        JsonArray defaultStoragePath = new JsonArray(Arrays.asList("data", "ticks"));
        int defaultClientId = 1;
        String defaultHost = "127.0.0.1";
        int defaultPort = 7497;
        JsonObject jsonConfig = new JsonObject()
                .put(IBROKERS_TICKS_STORAGE_PATH, defaultStoragePath)
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
        processHistoricalQuotes(vertx, dirs, 12087817, new QuoteProcessor(){
            @Override
            public void processQuote(Quote quote) {
                logger.info("sending: " + quote);
                JsonObject quoteJson = QuoteConverter.toJSON(quote);
                vertx.eventBus().send(HistoricalDataVerticle.ADDRESS_PROVIDE_HISTORY, quoteJson);
            }
        });

        /*
        Verticle historicalDataVerticle = new HistoricalDataVerticle();
        Observable<String> historicalDataDeployment = RxHelper.deployVerticle(vertx, historicalDataVerticle, options);
        historicalDataDeployment.subscribe(x -> {
            JsonObject historyRequest = new JsonObject();
            historyRequest.put("productCode", 12087817);
            historyRequest.put("replyTo", "TEST");
            logger.info("sending hist data feed request");
            vertx.eventBus().send(HistoricalDataVerticle.ADDRESS_PROVIDE_HISTORY, historyRequest);
        });
        */

    }

    /**
     * Waits for emission of all elements from stream1 before emitting elements from stream2.
     * Similar to concat but with no need for both streams to return identical types.
     *
     *     Observable<Integer> stream1 = Observable.from(new Integer[]{1,2,3,4});
     *     Observable<String> stream2 = Observable.from(new String[]{"a","b","c","d", "e"});
     *     chain(stream1.doOnNext(System.out::println), stream2).subscribe(System.out::println);
     *     > 1
     *     > 2
     *     > 3
     *     > 4
     *     > a
     *     > b
     *     > c
     *     > d
     *     > e
     *
     * @param stream1
     * @param stream2
     * @param <T1>
     * @param <T2>
     * @return Observable emitting elements from stream2 only after stream1 completion
     */
    private static <T1, T2> Observable<T2> chain(Observable<T1> stream1, Observable<T2> stream2) {
        Observable<T1> last = stream1
                .last();
        return combineLatest(last, stream2, (x, y) -> y);
    }

}
