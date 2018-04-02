package org.omarket.trading.verticles;

import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.core.eventbus.MessageConsumer;
import joinery.DataFrame;
import lombok.extern.slf4j.Slf4j;
import org.omarket.trading.ContractDBService;
import org.omarket.trading.Security;
import org.omarket.trading.quote.QuoteConverter;
import org.omarket.trading.quote.Quote;
import org.springframework.beans.factory.annotation.Autowired;
import rx.Observable;
import rx.exceptions.Exceptions;
import rx.functions.Action1;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.text.ParseException;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;

import static org.omarket.trading.quote.QuoteFactory.createFrom;

/**
 * Created by Christophe on 18/11/2016.
 */
@Slf4j
abstract class AbstractStrategyVerticle extends AbstractVerticle implements QuoteProcessor {


    @Autowired
    private ContractDBService contractDBService;
    static final String KEY_PRODUCT_CODES = "productCodes";
    static final String KEY_REPLY_TO = "replyTo";
    static final String KEY_COMPLETION_ADDRESS = "completionAddress";

    private static final String ADDRESS_HISTORICAL_QUOTES_PREFIX = "oot.historicalData.quote";
    private static final String ADDRESS_REALTIME_START_PREFIX = "oot.realtime.start";

    private JsonObject parameters = new JsonObject();

    private static DataFrame<Object> createSamplesDataFrame(Deque<Quote> quoteSamples) {
        Observable<Quote> quotesStream = Observable.from(quoteSamples);
        Observable<Integer> volBidStream = quotesStream.map(Quote::getBestBidSize);
        Observable<BigDecimal> bidStream = quotesStream.map(Quote::getBestBidPrice);
        Observable<BigDecimal> askStream = quotesStream.map(Quote::getBestAskPrice);
        Observable<Integer> volAskStream = quotesStream.map(Quote::getBestAskSize);
        Observable<List<Object>> dataStream = Observable
                .zip(volBidStream, bidStream, askStream, volAskStream,
                        (volBid, bid, ask, volAsk) -> Arrays.asList(volBid, bid, ask, volAsk));
        List<ZonedDateTime> indices = quotesStream.map(Quote::getLastModified)
                .collect(LinkedList<ZonedDateTime>::new, LinkedList<ZonedDateTime>::add)
                .toBlocking().first();
        List<String> columns = Arrays.asList("volume_bid", "bid", "ask", "volume_ask");
        DataFrame<Object> df = new DataFrame<>(columns, indices, dataStream.toBlocking().getIterator());
        return df.transpose();
    }

    abstract protected String[] getProductCodes();

    /*
     *
     */
    abstract protected void init();

    abstract protected Integer getSampledDataSize();

    abstract protected ChronoUnit getSampleDataUnit();

    JsonObject getParameters() {
        return parameters;
    }

    private Map<String, Security> createProducts() throws IOException {
        JsonArray pathElements = config().getJsonArray(VerticleProperties.PROPERTY_CONTRACT_DB_PATH);
        String contractDBPathName = String.join(File.separator, pathElements.getList());
        Path contractDBPath = FileSystems.getDefault().getPath(contractDBPathName);
        String[] productCodes = getProductCodes();
        Map<String, Security> products = new HashMap<>();
        for(String productCode: productCodes){
            Security contract = contractDBService.loadContract(contractDBPath, productCode);
            products.put(productCode, contract);
        }
        return products;
    }

    private String getHistoricalQuotesAddress() {
        return ADDRESS_HISTORICAL_QUOTES_PREFIX + "." + this.deploymentID();
    }

    private String getRealtimeQuotesAddress() {
        return ADDRESS_REALTIME_START_PREFIX + "." + this.deploymentID();
    }

    @Override
    public void start() {
        Observable<Integer> initStream = vertx.executeBlockingObservable(future -> {
            try {
                init();
                MessageConsumer<JsonObject> historicalTickDataConsumer = vertx.eventBus().consumer(getHistoricalQuotesAddress());
                Observable<JsonObject> historicalTickDataStream = historicalTickDataConsumer.bodyStream().toObservable();
                Observable<Quote> tickStream = historicalTickDataStream
                        .map(quoteJson -> {
                            try {
                                return QuoteConverter.fromJSON(quoteJson);
                            } catch (ParseException e) {
                                throw Exceptions.propagate(e);
                            }
                        });

                Map<String, Security> contracts = createProducts();
                final QuoteProcessor tickDataProcessor = new QuoteProcessor(getSampleDataUnit(), contracts);
                tickStream.subscribe(
                                tickDataProcessor,
                                error -> {
                                    log.error("error occured during backtest", error);
                                });

                MessageConsumer<JsonObject> realtimeStartConsumer = vertx.eventBus().consumer(getRealtimeQuotesAddress());
                Observable<JsonObject> realtimeStartStream = realtimeStartConsumer.bodyStream().toObservable();
                realtimeStartStream.subscribe(message -> {
                    log.info("unregistering historical data consumer");
                    historicalTickDataConsumer.unregister();
                    log.info("starting realtime processing");
                    // TODO - enable realtime processing: subscribe to market data using quoteProcessor
                });
                log.info("initialization completed");
                future.complete();
            } catch (Exception e) {
                log.error("failed to initialize strategy", e);
                future.fail(e);
            }
        });
        initStream
                .doOnCompleted(() -> { // requesting historical data
                    JsonObject request = new JsonObject();
                    Set<String> productCodes = null;
                    try {
                        productCodes = createProducts().keySet();
                        JsonArray codes = new JsonArray();
                        for (String code : productCodes) {
                            codes.add(code);
                        }
                        request.put(KEY_PRODUCT_CODES, codes);
                        request.put(KEY_REPLY_TO, getHistoricalQuotesAddress());
                        request.put(KEY_COMPLETION_ADDRESS, getRealtimeQuotesAddress());
                        log.info("requesting historical data for products: " + productCodes + " on address: " + HistoricalDataVerticle.ADDRESS_PROVIDE_HISTORY);
                        vertx.eventBus().send(HistoricalDataVerticle.ADDRESS_PROVIDE_HISTORY, request);
                    } catch (IOException e) {
                        log.error("failed to access product description", e);
                    }
                })
                .doOnError(error -> {
                    log.error("initialization step failed for " + this.deploymentID());
                })
                .subscribe();
    }

    private class QuoteHistory {
        final Map<String, Deque<Quote>> quotesByProductCode;
        private final Integer capacity;

        QuoteHistory(Integer capacity) {
            this.capacity = capacity;
            this.quotesByProductCode = new HashMap<>();
        }

        Quote addQuote(Quote quote) {
            String productCode = quote.getProductCode();
            if (!quotesByProductCode.containsKey(productCode)) {
                quotesByProductCode.put(productCode, new LinkedList<>());
            }
            Deque<Quote> quotes = quotesByProductCode.get(productCode);
            if (quotes.size() >= capacity) {
                quotes.poll();
            }
            quotes.add(quote);
            return quote;
        }

        Map<String, Deque<Quote>> getQuotes() {
            return quotesByProductCode;
        }

        public String toString(){
            return quotesByProductCode.toString();
        }
    }

    private class SampledQuoteHistory extends QuoteHistory {
        private final ChronoUnit samplingUnit;
        private Map<String, Quote> prevQuotes = new HashMap<>();

        SampledQuoteHistory(Integer capacity, ChronoUnit samplingUnit) {
            super(capacity);
            this.samplingUnit = samplingUnit;
        }
        private Quote first(String productCode) {
            Deque<Quote> quotes = quotesByProductCode.get(productCode);
            if (quotes == null || quotes.size() == 0) {
                return null;
            }
            return quotes.getFirst();
        }

        private Quote last(String productCode) {
            Deque<Quote> quotes = quotesByProductCode.get(productCode);
            if (quotes == null || quotes.size() == 0) {
                return null;
            }
            return quotes.getLast();
        }
        void addQuoteSampled(Quote quote) {
            String productCode = quote.getProductCode();
            Quote prevQuote = prevQuotes.getOrDefault(productCode, null);
            if (prevQuote != null && !quote.sameSampledTime(prevQuote, samplingUnit)) {
                log.info("prev quote:" + prevQuote.getLastModified());
                log.info("current quote:" + quote.getLastModified());
                ZonedDateTime samplingTime = quote.getLastModified().minus(1, samplingUnit);
                log.info("adding new sample for " + samplingTime);
                Quote newQuote = createFrom(prevQuote, samplingTime, samplingUnit);
                ZonedDateTime lastModified = newQuote.getLastModified();
                ZonedDateTime endDateTime = lastModified.minus(1, samplingUnit);
                forwardFillQuotes(productCode, endDateTime);
                addQuote(newQuote);
                for(String currentProductCode: quotesByProductCode.keySet()){
                    if(currentProductCode.equals(productCode)){
                        continue;
                    }
                    forwardFillQuotes(currentProductCode, lastModified);
                }
            }
            prevQuotes.put(productCode, quote);
        }

        private void forwardFillQuotes(String productCode, ZonedDateTime endDateTime) {
            Quote first = first(productCode);
            if (first != null) {
                log.debug("filling samples from " + first.getLastModified() + " to " + endDateTime);
                Quote last = last(productCode);
                if (last == null) {
                    return;
                }
                log.debug("last time: " + last.getLastModified());
                while (last.getLastModified().isBefore(endDateTime)) {
                    Quote fillQuote = createFrom(last, samplingUnit, 1);
                    log.debug("filling with sample for: " + fillQuote.getLastModified());
                    last = addQuote(fillQuote);
                }
            }
        }

        Map<String, DataFrame>  getDataFrames(){
                Map<String, DataFrame> samplesDataframe = new HashMap<>();
                getQuotes().forEach((productCode, quotes) -> {
                    samplesDataframe.put(productCode, createSamplesDataFrame(quotes));
                });
                return samplesDataframe;
        }
    }

    private class QuoteProcessor implements Action1<Quote> {
        private final SampledQuoteHistory sampleQuotes;
        private final QuoteHistory quotes;
        private final Map<String, Security> contracts;

        QuoteProcessor(ChronoUnit samplingUnit, Map<String, Security> contracts) {
            this.sampleQuotes = new SampledQuoteHistory(getSampledDataSize(), samplingUnit);
            this.quotes = new QuoteHistory(getSampledDataSize());
            this.contracts = contracts;
        }

        @Override
        public void call(Quote quote) {
            sampleQuotes.addQuoteSampled(quote);
            log.info("samples for product " + quote.getProductCode() + ":\n" + sampleQuotes);
            quotes.addQuote(quote);
            log.info("forwarding order book to concrete strategy after update from: " + quote);
            processQuotes(contracts, quotes.getQuotes(), sampleQuotes.getDataFrames());
        }
    }

}
