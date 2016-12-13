package org.omarket.trading.verticles;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.ib.client.Contract;
import com.ib.client.ContractDetails;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.Future;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.omarket.trading.quote.QuoteConverter;
import org.omarket.trading.quote.Quote;
import rx.Observable;

import java.util.*;

import static org.omarket.trading.MarketData.createChannelQuote;
import static org.omarket.trading.MarketData.processBacktest;
import static org.omarket.trading.verticles.MarketDataVerticle.ADDRESS_CONTRACT_RETRIEVE;

/**
 * Created by Christophe on 01/11/2016.
 */
public class FakeMarketDataVerticle extends AbstractVerticle {
    private final static Logger logger = LoggerFactory.getLogger(FakeMarketDataVerticle.class.getName());

    private final static Integer[] IB_CODES = new Integer[]{12087817, 12087820, 28027110, 37893488};
    public static final String IBROKERS_TICKS_STORAGE_PATH = "ibrokers.ticks.storagePath";

    public void start() throws Exception {
        logger.info("starting market data");
        JsonArray storageDirs = config().getJsonArray(IBROKERS_TICKS_STORAGE_PATH);
        List<String> dirs = storageDirs.getList();

        Map<Integer, Queue<Quote>> quotes = new HashMap<>();
        Observable<Object> x = vertx.executeBlockingObservable(future -> {
            try {
                processContractRetrieve(vertx);

                for (Integer ibCode : IB_CODES) {
                    quotes.put(ibCode, new LinkedList<>());
                    processBacktest(dirs, ibCode, new StrategyProcessor() {

                        @Override
                        public void processQuote(Quote quote, boolean isBacktest) {
                            quotes.get(ibCode).add(quote);
                        }

                        @Override
                        public void updateQuotes(Quote quotePrev) {

                        }
                    });
                }
                future.complete();
            } catch (Exception e) {
                logger.error("failed to initialize strategy", e);
                future.fail(e);
            }
        });
        x.subscribe(y -> {
            logger.info("subscribing: " + y);
            for (Integer ibCode : IB_CODES) {
                final String channel = createChannelQuote(ibCode);
                vertx.periodicStream(1000).
                        toObservable().
                        subscribe(
                                id -> {
                                    Quote quote = quotes.get(ibCode).remove();
                                    logger.info("sending quote: " + quote);
                                    vertx.eventBus().send(channel, QuoteConverter.toJSON(quote));
                                }
                        );
            }
            //startFuture.complete();
        }, err -> {
            //startFuture.fail("failed to load order books: skipping");
            logger.error("failed to load order books: skipping");
        });
    }

    private static void processContractRetrieve(Vertx vertx) {
        Observable<Message<JsonObject>> contractStream = vertx.eventBus().<JsonObject>consumer(ADDRESS_CONTRACT_RETRIEVE).toObservable();
        contractStream.subscribe(message -> {
            logger.info("faking contract retrieve: " + message.body());
            Contract contract = new Contract();
            contract.conid(Integer.valueOf(message.body().getString("conId")));
            ContractDetails details = new ContractDetails();
            details.contract(contract);
            Gson gson = new GsonBuilder().create();
            JsonObject product = new JsonObject(gson.toJson(details));
            message.reply(product);
        });
        logger.info("Fake market data ready to provide contract details");
    }
}
