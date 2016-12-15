package org.omarket.trading.verticles;

import com.ib.client.Contract;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.rx.java.ObservableFuture;
import io.vertx.rx.java.RxHelper;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.eventbus.Message;
import org.omarket.trading.ibrokers.IBrokersConnectionFailure;
import org.omarket.trading.ibrokers.IBrokersMarketDataCallback;
import org.omarket.trading.quote.Quote;
import rx.Observable;

import java.io.File;
import java.math.BigDecimal;
import java.math.MathContext;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.*;

import static org.omarket.trading.MarketData.IBROKERS_TICKS_STORAGE_PATH;
import static org.omarket.trading.MarketData.processBacktest;

/**
 * Created by Christophe on 01/11/2016.
 */
public class HistoricalDataVerticle extends AbstractVerticle {
    private final static Logger logger = LoggerFactory.getLogger(HistoricalDataVerticle.class.getName());
    public final static String ADDRESS_PROVIDE_HISTORY = "oot.historicalData.provide";

    public void start(Future<Void> startFuture) throws Exception {
        logger.info("starting historical data");
        String storageDirPathName = String.join(File.separator, config().getJsonArray(IBROKERS_TICKS_STORAGE_PATH).getList());
        Path storageDirPath = FileSystems.getDefault().getPath(storageDirPathName);
        logger.info("ticks data storage set to '" + storageDirPath + "'");

        JsonArray storageDirs = config().getJsonArray(IBROKERS_TICKS_STORAGE_PATH);
        List<String> dirs = storageDirs.getList();

        Observable<JsonObject> contractStream = vertx.eventBus().<JsonObject>consumer(ADDRESS_PROVIDE_HISTORY).bodyStream().toObservable();
        contractStream.subscribe(message -> {
            final Integer ibCode = message.getInteger("productCode");
            final String address = message.getString("replyTo");
            logger.info("data for contract " + ibCode + " will be sent to " + address);

            processBacktest(dirs, ibCode, new QuoteProcessor() {

                @Override
                public void processQuote(Quote quote) {
                    logger.info("processing: " + quote);
                }

            });
        });
        startFuture.complete();
    }

}
