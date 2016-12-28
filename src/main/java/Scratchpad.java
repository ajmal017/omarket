
import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.core.Vertx;
import joinery.DataFrame;
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

import java.io.*;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.Thread.sleep;
import static org.omarket.trading.MarketData.IBROKERS_TICKS_STORAGE_PATH;
import static org.omarket.trading.verticles.HistoricalDataVerticle.mergeQuoteStreams;
import static org.omarket.trading.verticles.HistoricalDataVerticle.getHistoricalQuoteStream;
import static rx.Observable.combineLatest;
import static rx.Observable.interval;
import static yahoofinance.Utils.loadObject;
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
        Map<String, Stock> stocks = YahooFinance.get(symbols, from);
        Stock ewc = stocks.get("EWC");
        Stock ewa = stocks.get("EWA");
        Stock uso = stocks.get("USO");
        List<HistoricalQuote> historyEWC = ewc.getHistory(Interval.DAILY);
        List<HistoricalQuote> historyEWA = ewa.getHistory(Interval.DAILY);
        List<HistoricalQuote> historyUSO = uso.getHistory(Interval.DAILY);
        storeObject(Paths.get("data", "ewc-hist.bin"), historyEWC);
        storeObject(Paths.get("data", "ewa-hist.bin"), historyEWA);
        storeObject(Paths.get("data", "uso-hist.bin"), historyUSO);
    }

    public static void main(String[] args) throws Exception {
        List<HistoricalQuote> historyEWC = loadObject(Paths.get("data", "ewc-hist.bin"));
        List<HistoricalQuote> historyEWA = loadObject(Paths.get("data", "ewa-hist.bin"));
        List<HistoricalQuote> historyUSO = loadObject(Paths.get("data", "uso-hist.bin"));
        Stream<Date> indexStreamEWC = historyEWC.stream()
                .map(HistoricalQuote::getDate)
                .map(Calendar::getTime);
        Stream<Date> indexStreamEWA = historyEWA.stream()
                .map(HistoricalQuote::getDate)
                .map(Calendar::getTime);
        Stream<List<BigDecimal>> adjCloseEWC = historyEWC.stream()
                .map(HistoricalQuote::getAdjClose)
                .map(Collections::singletonList);
        Stream<List<BigDecimal>> adjCloseEWA = historyEWA.stream()
                .map(HistoricalQuote::getAdjClose)
                .map(Collections::singletonList);
        Collection<String> columnsEWC = Collections.singletonList("ewc");
        Collection<String> columnsEWA = Collections.singletonList("ewa");
        Collection<Date> indexEWC = indexStreamEWC.collect(Collectors.toList());
        Collection<Date> indexEWA = indexStreamEWA.collect(Collectors.toList());

        DataFrame ewcDf = new DataFrame(columnsEWC, indexEWC, adjCloseEWC.iterator()).transpose();
        DataFrame ewaDf = new DataFrame(columnsEWA, indexEWA, adjCloseEWA.iterator()).transpose();
        logger.info("df:\n" + ewcDf);
        logger.info("df:\n" + ewaDf);
        DataFrame df = ewcDf.join(ewaDf);
        logger.info("df:\n" + df);
    }

    public static void main1(String[] args) throws Exception {
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

}
