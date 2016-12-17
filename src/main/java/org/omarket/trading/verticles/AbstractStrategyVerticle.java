package org.omarket.trading.verticles;

import io.vertx.rx.java.ObservableFuture;
import io.vertx.rx.java.RxHelper;
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

import static org.omarket.trading.MarketData.IBROKERS_TICKS_STORAGE_PATH;
import static org.omarket.trading.MarketData.createChannelQuote;
import static org.omarket.trading.Util.chain;

/**
 * Created by Christophe on 18/11/2016.
 */

abstract class AbstractStrategyVerticle extends AbstractVerticle implements StrategyProcessor {
    private static final String PARAM_PAST_QUOTES = "pastQuotes";
    private final static Logger logger = LoggerFactory.getLogger(AbstractStrategyVerticle.class);

    private JsonObject parameters = new JsonObject();

    private Map<String, JsonObject> contracts = new HashMap<>();

    abstract protected String[] getProductCodes();

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
        List<Quote> quotesList = (List<Quote>) quotes.getList();
        return quotesList;
    }

    @Override
    public void start() {
        Observable<Integer> initStream = vertx.executeBlockingObservable(future -> {
            try {
                JsonArray array = new JsonArray();
                getParameters().put(PARAM_PAST_QUOTES, array);
                init(getLookBackPeriod());
                logger.info("initialization completed");
                future.complete();
            } catch (Exception e) {
                logger.error("failed to initialize strategy", e);
                future.fail(e);
            }
        });
        Observable<String> productCodes = chain(initStream, Observable.from(getProductCodes()));
        productCodes
                .forEach(productCode -> {
                    JsonObject request = new JsonObject();
                    request.put("productCode", productCode);
                    request.put("replyTo", "historical");
                    logger.info("requesting historical data for product: " + productCode);
                    vertx.eventBus().send(HistoricalDataVerticle.ADDRESS_PROVIDE_HISTORY, request);
                });
        /*
        Observable<String> productCodes = chain(initStream, Observable.from(getProductCodes()));
        productCodes
                .flatMap(productCode -> {
                    JsonObject product = new JsonObject().put("conId", productCode);
                    ObservableFuture<Message<JsonObject>> contractStream = RxHelper.observableFuture();
                    logger.info("requesting subscription for product: " + productCode);
                    vertx.eventBus().send(MarketDataVerticle.ADDRESS_CONTRACT_RETRIEVE, product, contractStream.toHandler());
                    return contractStream;
                })
                .zipWith(Observable.from(getProductCodes()), (contractMessage, productCode) -> {
                    JsonObject contract = contractMessage.body();
                    contracts.put(productCode, contract);
                    final String channelProduct = createChannelQuote(productCode);
                    return vertx.eventBus().<JsonObject>consumer(channelProduct).toObservable();
                })
                .flatMap(quote -> {
                    logger.info("flatmap: " + quote);
                    return quote;
                })
                .subscribe(new QuoteProcessor());
                */
    }

    private class QuoteProcessor implements Action1<Message<JsonObject>> {

        @Override
        public void call(Message<JsonObject> message) {
            try {
                Quote quote = QuoteConverter.fromJSON(message.body());
                if (contracts.size() != getProductCodes().length || quote == null) {
                    return;
                }
                logger.info("processing order book: " + quote);
                processQuote(quote);
                updateQuotes(quote);
            } catch (ParseException e) {
                logger.error("failed to parse tick data from " + message.body(), e);
            }
        }
    }

}

