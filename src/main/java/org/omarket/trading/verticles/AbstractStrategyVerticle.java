package org.omarket.trading.verticles;

import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.rxjava.core.eventbus.MessageConsumer;
import org.omarket.trading.quote.QuoteConverter;
import org.omarket.trading.quote.Quote;
import rx.Observable;
import rx.exceptions.Exceptions;
import rx.functions.Action1;

import java.text.ParseException;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.*;

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

    abstract protected Integer getTickHistorySize();

    Observable<List<Quote>> bufferize(Observable<Quote> quoteStream, Integer count) {
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

                MessageConsumer<JsonObject> historicalSampledDataConsumer = vertx.eventBus().consumer(getHistoricalQuotesAddress());
                Observable<JsonObject> historicalSampledDataStream = historicalSampledDataConsumer.bodyStream().toObservable();
                Observable<Quote> sampledStream = historicalSampledDataStream
                        .map(quoteJson -> {
                            try {
                                return QuoteConverter.fromJSON(quoteJson);
                            } catch (ParseException e) {
                                throw Exceptions.propagate(e);
                            }
                        })
                        .buffer(2, 1)
                        .filter(x -> !x.get(0).sameSampledTime(x.get(1), ChronoUnit.SECONDS))
                        .map(x -> x.get(0));
                final QuoteProcessor sampledDataProcessor = new QuoteProcessor();
                sampledStream.forEach(x -> logger.info("SAMPLED: " + x));

                final QuoteProcessor tickDataProcessor = new QuoteProcessor();
                bufferize(tickStream, getTickHistorySize())
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

        final Map<String, List<Quote>> quotes;

        QuoteProcessor() {
            this.quotes = new HashMap<>();
        }

        @Override
        public void call(List<Quote> quoteHistory) {
            Quote quote = quoteHistory.get(quoteHistory.size() - 1);
            String productCode = quote.getProductCode();
            quotes.put(productCode, quoteHistory);
            logger.debug("forwarding order book to concrete strategy after update from: " + quote);
            processQuotes(quotes);
        }
    }

}

