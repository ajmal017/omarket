package org.omarket.trading.verticles;

import io.vertx.rx.java.ObservableFuture;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.omarket.trading.MarketData;
import org.omarket.trading.quote.QuoteConverter;
import org.omarket.trading.quote.Quote;
import rx.Observable;
import rx.functions.Action1;

import java.text.ParseException;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;

import static org.omarket.trading.verticles.MarketDataVerticle.IBROKERS_TICKS_STORAGE_PATH;
import static org.omarket.trading.MarketData.createChannelQuote;

/**
 * Created by Christophe on 18/11/2016.
 */

abstract class AbstractStrategyVerticle extends AbstractVerticle implements StrategyProcessor {
    private static final String PARAM_PAST_QUOTES = "pastQuotes";
    private final static Logger logger = LoggerFactory.getLogger(AbstractStrategyVerticle.class);

    private JsonObject parameters = new JsonObject();

    private Map<Integer, JsonObject> contracts = new HashMap<>();

    abstract protected Integer[] getIBrokersCodes();

    /**
     * For how long back the verticle needs to keep past order books.
     *
     * @return lookback period in milliseconds
     */
    abstract protected Integer getLookBackPeriod();

    /*
     *
     */
    abstract protected void init(Integer lookbackPeriod);

    public void updateQuotes(Quote quote) {
        try {
            List<JsonObject> quotes = this.getParameters().getJsonArray(PARAM_PAST_QUOTES).getList();
            if (quotes.size() > 0) {
                Quote firstQuote = QuoteConverter.fromJSON(quotes.get(0));
                Quote lastQuote = QuoteConverter.fromJSON(quotes.get(quotes.size() - 1));
                logger.info("quotes range before update: " + firstQuote.getLastModified() + " / " + lastQuote.getLastModified());
            }
            ZonedDateTime lastModified = quote.getLastModified();
            ZonedDateTime expiry = lastModified.minus(getLookBackPeriod(), ChronoUnit.MILLIS);
            this.getParameters().put(PARAM_PAST_QUOTES, new JsonArray());
            for (JsonObject currentQuoteJson : quotes) {
                Quote currentQuote = QuoteConverter.fromJSON(currentQuoteJson);
                if (currentQuote.getLastModified().isAfter(expiry)) {
                    this.getParameters().getJsonArray(PARAM_PAST_QUOTES).add(QuoteConverter.toJSON(currentQuote));
                }
            }
            this.getParameters().getJsonArray(PARAM_PAST_QUOTES).add(QuoteConverter.toJSON(quote));
            logger.info("updated quote: " + quote);
        } catch (ParseException e) {
            logger.error("unable to update quote", e);
        }
    }

    JsonObject getParameters() {
        return parameters;
    }

    List<Quote> getPastQuotes() {
        JsonArray quotes = getParameters().getJsonArray(PARAM_PAST_QUOTES);
        List<Quote> quotesList = (List<Quote>)quotes.getList();
        return quotesList;
    }

    @Override
    public void start() {
        Observable<Object> initStream = vertx.executeBlockingObservable(future -> {
            try {
                JsonArray array = new JsonArray();
                getParameters().put(PARAM_PAST_QUOTES, array);
                init(getLookBackPeriod());
                future.complete();
            } catch (Exception e) {
                logger.error("failed to initialize strategy", e);
                future.fail(e);
            }
        });
        initStream.subscribe(new BacktestProcessor());
    }

    private class BacktestProcessor implements Action1<Object> {
        @Override
        public void call(Object onNext) {
            logger.info("initialized strategy");
            for (Integer ibCode : AbstractStrategyVerticle.this.getIBrokersCodes()) {
                Observable<Object> backtestStream = vertx.executeBlockingObservable(future -> {
                    boolean runBacktest = AbstractStrategyVerticle.this.config().getBoolean("runBacktestFlag", true);
                    if (runBacktest) {
                        logger.info("executing backtest");
                        JsonArray storageDirs = AbstractStrategyVerticle.this.config().getJsonArray(IBROKERS_TICKS_STORAGE_PATH);
                        List<String> dirs = storageDirs.getList();
                        try {
                            MarketData.processBacktest(dirs, ibCode, AbstractStrategyVerticle.this);
                            future.complete();
                        } catch (Exception e) {
                            logger.error("backtest failed", e);
                            future.fail(e);
                        }
                    } else {
                        logger.info("skipping backtest");
                        future.complete();
                    }
                });

                backtestStream.subscribe(result -> {
                    logger.info("processing realtime quotes for " + ibCode);
                    // forwards quotes to strategy processor
                    final String channelProduct = createChannelQuote(ibCode);
                    Observable<Message<JsonObject>> quotesStream = vertx.eventBus().<JsonObject>consumer(channelProduct).toObservable();
                    quotesStream.subscribe(new QuoteProcessor());
                });

                ObservableFuture<Message<JsonObject>> contractStream = MarketDataVerticle.createContractStream(vertx, ibCode);
                contractStream.subscribe(new RequestSubscription(ibCode),
                        onError -> {
                            logger.error("failed to retrieve contract details: ", onError);
                        }
                );
            }
        }
    }

    private class QuoteProcessor implements Action1<Message<JsonObject>> {

        @Override
        public void call(Message<JsonObject> message) {
            try {
                Quote quote = QuoteConverter.fromJSON(message.body());
                if (contracts.size() != AbstractStrategyVerticle.this.getIBrokersCodes().length || quote == null) {
                    return;
                }
                logger.info("processing order book: " + quote);
                AbstractStrategyVerticle.this.processQuote(quote, false);
                AbstractStrategyVerticle.this.updateQuotes(quote);
            } catch (ParseException e) {
                logger.error("failed to parse tick data from " + message.body(), e);
            }
        }
    }

    private class RequestSubscription implements Action1<Message<JsonObject>> {
        private final Integer ibCode;

        RequestSubscription(Integer ibCode) {
            this.ibCode = ibCode;
        }

        @Override
        public void call(Message<JsonObject> contractMessage) {
            JsonObject contract = contractMessage.body();
            vertx.eventBus().send(MarketDataVerticle.ADDRESS_SUBSCRIBE_TICK, contract, mktDataReply -> {
                logger.info("subscription succeeded for product: " + ibCode);
                contracts.put(ibCode, contract);
            });
        }
    }
}

