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

    private static final String ADDRESS_HISTORICAL_QUOTES_PREFIX = "oot.historicalData.quote";
    private final static Logger logger = LoggerFactory.getLogger(AbstractStrategyVerticle.class);

    private JsonObject parameters = new JsonObject();

    abstract protected String[] getProductCodes();

    /*
     *
     */
    abstract protected void init();

    JsonObject getParameters() {
        return parameters;
    }

    public String getHistoricalQuotesAddress(){
        return ADDRESS_HISTORICAL_QUOTES_PREFIX + "." + this.getClass().getSimpleName();
    }

    @Override
    public void start() {
        Observable<Integer> initStream = vertx.executeBlockingObservable(future -> {
            try {
                init();
                Observable<Message<JsonObject>> historicalDataStream = vertx.eventBus().<JsonObject>consumer(getHistoricalQuotesAddress()).toObservable();
                historicalDataStream.subscribe(
                        new QuoteProcessor(),
                        error->{
                            logger.error("error occured during backtest");
                        },
                        () -> {
                            logger.info("completed backtest");
                        });
                logger.info("initialization completed");
                future.complete();
            } catch (Exception e) {
                logger.error("failed to initialize strategy", e);
                future.fail(e);
            }
        });
        initStream
                .doOnCompleted(() -> {
                Observable.from(getProductCodes())
                        .forEach(productCode -> {
                            JsonObject request = new JsonObject();
                            request.put("productCode", productCode);
                            request.put("replyTo", getHistoricalQuotesAddress());
                            logger.info("requesting historical data for product: " + productCode + " on address: " + HistoricalDataVerticle.ADDRESS_PROVIDE_HISTORY);
                            vertx.eventBus().send(HistoricalDataVerticle.ADDRESS_PROVIDE_HISTORY, request);
                        });
                }
                )
                .subscribe();
    }

    private class QuoteProcessor implements Action1<Message<JsonObject>> {

        @Override
        public void call(Message<JsonObject> message) {
            try {
                Quote quote = QuoteConverter.fromJSON(message.body());
                logger.info("forwarding order book to implementing strategy: " + quote);
                processQuote(quote);
            } catch (ParseException e) {
                logger.error("failed to parse tick data from " + message.body(), e);
            }
        }
    }

}

