package org.omarket.trading.verticles;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.ib.client.Contract;
import com.ib.client.ContractDetails;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.omarket.trading.quote.QuoteConverter;
import org.omarket.trading.quote.Quote;

import java.util.*;

import static org.omarket.trading.MarketData.createChannelQuote;
import static org.omarket.trading.MarketData.processBacktest;

/**
 * Created by Christophe on 01/11/2016.
 */
public class FakeMarketDataVerticle extends AbstractVerticle {
    private final static Logger logger = LoggerFactory.getLogger(FakeMarketDataVerticle.class.getName());

    private final static Integer IB_CODE = 12087817;
    public static final String IBROKERS_TICKS_STORAGE_PATH = "ibrokers.ticks.storagePath";


    @SuppressWarnings("unchecked")
    public void start(Future<Void> startFuture) throws Exception {
        logger.info("starting market data");
        JsonArray storageDirs = config().getJsonArray(IBROKERS_TICKS_STORAGE_PATH);
        List<String> dirs = storageDirs.getList();
        final String channel = createChannelQuote(IB_CODE);

        Queue<Quote> quotes = new LinkedList<>();
        vertx.executeBlocking(future -> {
            try {
                processContractRetrieve(vertx);
                processBacktest(dirs, IB_CODE, new StrategyProcessor(){

                    @Override
                    public void processQuote(Quote quote, boolean isBacktest) {
                        quotes.add(quote);
                    }

                    @Override
                    public void updateQuotes(Quote quotePrev) {

                    }
                });
                future.complete();

            } catch (Exception e) {
                logger.error("failed to initialize strategy", e);
                future.fail(e);
            }
        }, completed -> {
            if(completed.succeeded()) {
                startFuture.complete();
                vertx.setPeriodic(1000, id -> {
                    if(quotes.size() > 0) {
                        Quote quote = quotes.remove();
                        logger.info("sending quote: " + quote);
                        vertx.eventBus().send(channel, QuoteConverter.toJSON(quote));
                    } else {
                        // todo: interrupt timer
                        logger.info("queue empty: skipping");
                    }
                });
            } else {
                startFuture.fail("failed to load order books: skipping");
                logger.error("failed to load order books: skipping");
            }
        }
        );
    }

    private static void processContractRetrieve(Vertx vertx) {
        MessageConsumer<JsonObject> consumer = vertx.eventBus().consumer(MarketDataVerticle.ADDRESS_CONTRACT_RETRIEVE);
        consumer.handler(message -> {
            logger.info("faking contract retrieve: " + message.body());
            Contract contract = new Contract();
            contract.conid(IB_CODE);
            ContractDetails details = new ContractDetails();
            details.contract(contract);
            Gson gson = new GsonBuilder().create();
            JsonObject product = new JsonObject(gson.toJson(details));
            message.reply(product);
        });
    }
}
