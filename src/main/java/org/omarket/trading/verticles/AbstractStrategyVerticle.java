package org.omarket.trading.verticles;

import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.rxjava.core.eventbus.MessageConsumer;
import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.omarket.trading.quote.QuoteConverter;
import org.omarket.trading.quote.Quote;
import rx.Observable;
import rx.exceptions.Exceptions;
import rx.functions.Action1;

import java.text.ParseException;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;

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

    abstract protected String[] getProductCodes();

    /*
     *
     */
    abstract protected void init();

    abstract protected Integer getSampledDataSize();
    abstract protected ChronoUnit getSampleDataUnit();

    private Observable<List<Quote>> bufferize(Observable<Quote> quoteStream, Integer count) {
        return quoteStream.buffer(count, 1);
    }

    JsonObject getParameters() {
        return parameters;
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

                final QuoteProcessor tickDataProcessor = new QuoteProcessor(getSampleDataUnit());
                bufferize(tickStream, 2) // forwards previous quote together with current quote
                        .subscribe(
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
                    String[] productCodes = getProductCodes();
                    JsonArray codes = new JsonArray();
                    for (String code : productCodes) {
                        codes.add(code);
                    }
                    request.put(KEY_PRODUCT_CODES, codes);
                    request.put(KEY_REPLY_TO, getHistoricalQuotesAddress());
                    request.put(KEY_COMPLETION_ADDRESS, getRealtimeQuotesAddress());
                    logger.info("requesting historical data for products: " + Arrays.asList(productCodes) + " on address: " + HistoricalDataVerticle.ADDRESS_PROVIDE_HISTORY);
                    vertx.eventBus().send(HistoricalDataVerticle.ADDRESS_PROVIDE_HISTORY, request);
                })
                .doOnError(error -> {
                    logger.error("initialization step failed for " + this.deploymentID());
                })
                .subscribe();
    }

    private class QuoteProcessor implements Action1<List<Quote>> {

        final Map<String, Queue<Quote>> sampledQuotesByProductCode;
        final Map<String, Quote> latestQuotesByProductCode;
        final ChronoUnit samplingUnit;

        QuoteProcessor(ChronoUnit samplingUnit) {
            this.samplingUnit = samplingUnit;
            this.sampledQuotesByProductCode = new HashMap<>();
            this.latestQuotesByProductCode = new HashMap<>();
        }

        @Override
        public void call(List<Quote> quoteHistory) {
            Quote quote = quoteHistory.get(1);
            Quote prevQuote = quoteHistory.get(0);
            String productCode = quote.getProductCode();
            latestQuotesByProductCode.put(productCode, quote);
            if (!quote.sameSampledTime(prevQuote, samplingUnit)){
                Queue<Quote> sampledQuotes = sampledQuotesByProductCode.getOrDefault(productCode, new CircularFifoQueue<>(getSampledDataSize()));
                Quote newQuote = createFrom(prevQuote, quote.getLastModified(), samplingUnit);
                ZonedDateTime endDateTime = newQuote.getLastModified().minus(1, samplingUnit);
                if (sampledQuotes.peek() != null){
                    logger.info("filling samples from " + sampledQuotes.peek().getLastModified() + " to " + endDateTime);
                    while(sampledQuotes.peek().getLastModified().isBefore(endDateTime)){
                        Quote fillQuote = createFrom(sampledQuotes.peek(), samplingUnit, 1);
                        logger.info("samples queue: " + sampledQuotes);
                        logger.info("filling with sample for: " + fillQuote.getLastModified());
                        sampledQuotes.add(fillQuote);
                    }
                }
                logger.info("adding new sample for " + newQuote.getLastModified());
                sampledQuotes.add(newQuote);
                sampledQuotesByProductCode.put(productCode, sampledQuotes);
            }
            logger.debug("forwarding order book to concrete strategy after update from: " + quote);
            processQuotes(latestQuotesByProductCode, sampledQuotesByProductCode);
        }
    }

}

