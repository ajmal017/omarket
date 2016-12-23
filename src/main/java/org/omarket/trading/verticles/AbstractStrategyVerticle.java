package org.omarket.trading.verticles;

import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.rxjava.core.eventbus.MessageConsumer;
import joinery.DataFrame;
import org.omarket.trading.MarketData;
import org.omarket.trading.quote.QuoteConverter;
import org.omarket.trading.quote.Quote;
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

import static org.omarket.trading.MarketData.loadIBrokersProductDescription;
import static org.omarket.trading.quote.QuoteFactory.createFrom;

/**
 * Created by Christophe on 18/11/2016.
 */

abstract class AbstractStrategyVerticle extends AbstractVerticle implements QuoteProcessor {
    private final static Logger logger = LoggerFactory.getLogger(AbstractStrategyVerticle.class);

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

    private Map<String, JsonObject> createProducts() throws IOException {
        JsonArray pathElements = config().getJsonArray(MarketData.IBROKERS_TICKS_STORAGE_PATH);
        String storageDirPathName = String.join(File.separator, pathElements.getList());
        Path storageDirPath = FileSystems.getDefault().getPath(storageDirPathName);
        String[] productCodes = getProductCodes();
        Map<String, JsonObject> products = new HashMap<>();
        for(String productCode: productCodes){
            JsonObject product = loadIBrokersProductDescription(storageDirPath, productCode);
            products.put(productCode, product);
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

                Map<String, JsonObject> contracts = createProducts();
                final QuoteProcessor tickDataProcessor = new QuoteProcessor(getSampleDataUnit(), contracts);
                tickStream.subscribe(
                                tickDataProcessor,
                                error -> {
                                    logger.error("error occured during backtest", error);
                                });

                MessageConsumer<JsonObject> realtimeStartConsumer = vertx.eventBus().consumer(getRealtimeQuotesAddress());
                Observable<JsonObject> realtimeStartStream = realtimeStartConsumer.bodyStream().toObservable();
                realtimeStartStream.subscribe(message -> {
                    logger.info("unregistering historical data consumer");
                    historicalTickDataConsumer.unregister();
                    logger.info("starting realtime processing");
                    // TODO - enable realtime processing: subscribe to market data using quoteProcessor
                });
                logger.info("initialization completed");
                future.complete();
            } catch (Exception e) {
                logger.error("failed to initialize strategy", e);
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
                        logger.info("requesting historical data for products: " + productCodes + " on address: " + HistoricalDataVerticle.ADDRESS_PROVIDE_HISTORY);
                        vertx.eventBus().send(HistoricalDataVerticle.ADDRESS_PROVIDE_HISTORY, request);
                    } catch (IOException e) {
                        logger.error("failed to access product description", e);
                    }
                })
                .doOnError(error -> {
                    logger.error("initialization step failed for " + this.deploymentID());
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

        Quote addQuote(String productCode, Quote quote) {
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
    }

    private class SampledQuoteHistory extends QuoteHistory {
        private final ChronoUnit samplingUnit;
        private Quote prevQuote = null;

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
        void addQuoteSampled(String productCode, Quote quote) {
            if (prevQuote != null && !quote.sameSampledTime(prevQuote, samplingUnit)) {
                Quote newQuote = createFrom(prevQuote, quote.getLastModified(), samplingUnit);
                ZonedDateTime lastModified = newQuote.getLastModified();
                ZonedDateTime endDateTime = lastModified.minus(1, samplingUnit);
                forwardFillQuotes(productCode, endDateTime);
                logger.debug("adding new sample for " + lastModified);
                addQuote(productCode, newQuote);
                for(String currentProductCode: quotesByProductCode.keySet()){
                    if(currentProductCode.equals(productCode)){
                        continue;
                    }
                    forwardFillQuotes(currentProductCode, lastModified);
                }
            }
            prevQuote = quote;
        }

        private void forwardFillQuotes(String productCode, ZonedDateTime endDateTime) {
            Quote first = first(productCode);
            if (first != null) {
                logger.debug("filling samples from " + first.getLastModified() + " to " + endDateTime);
                Quote last = last(productCode);
                if (last == null) {
                    return;
                }
                logger.debug("last time: " + last.getLastModified());
                while (last.getLastModified().isBefore(endDateTime)) {
                    Quote fillQuote = createFrom(last, samplingUnit, 1);
                    logger.debug("filling with sample for: " + fillQuote.getLastModified());
                    last = addQuote(productCode, fillQuote);
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
        private final Map<String, JsonObject> contracts;

        QuoteProcessor(ChronoUnit samplingUnit, Map<String, JsonObject> contracts) {
            this.sampleQuotes = new SampledQuoteHistory(getSampledDataSize(), samplingUnit);
            this.quotes = new QuoteHistory(getSampledDataSize());
            this.contracts = contracts;
        }

        @Override
        public void call(Quote quote) {
            String productCode = quote.getProductCode();
            sampleQuotes.addQuoteSampled(productCode, quote);
            quotes.addQuote(productCode, quote);
            logger.debug("forwarding order book to concrete strategy after update from: " + quote);
            processQuotes(contracts, quotes.getQuotes(), sampleQuotes.getDataFrames());
        }
    }

}
