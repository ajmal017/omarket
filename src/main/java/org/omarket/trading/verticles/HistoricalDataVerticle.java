package org.omarket.trading.verticles;

import com.ib.client.Contract;
import io.vertx.core.Future;
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
import rx.Observable;

import java.io.File;
import java.math.BigDecimal;
import java.math.MathContext;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.omarket.trading.MarketData.IBROKERS_TICKS_STORAGE_PATH;

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

        Observable<JsonObject> contractStream = vertx.eventBus().<JsonObject>consumer(ADDRESS_PROVIDE_HISTORY).bodyStream().toObservable();
        contractStream.subscribe(ibCodeJson -> {
            final Integer ibCode = ibCodeJson.getInteger("ibCode");
            logger.info("registering contract: " + ibCode);
        });
        startFuture.complete();
    }

}
