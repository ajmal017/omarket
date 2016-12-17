package org.omarket.trading.verticles;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.ib.client.Contract;
import com.opencsv.CSVReader;
import io.vertx.core.Future;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.rx.java.ObservableFuture;
import io.vertx.rx.java.RxHelper;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.buffer.Buffer;
import io.vertx.rxjava.core.eventbus.Message;
import io.vertx.rxjava.core.file.AsyncFile;
import io.vertx.rxjava.core.file.FileSystem;
import org.omarket.trading.MarketData;
import org.omarket.trading.ibrokers.IBrokersConnectionFailure;
import org.omarket.trading.ibrokers.IBrokersMarketDataCallback;
import org.omarket.trading.quote.Quote;
import org.omarket.trading.quote.QuoteConverter;
import org.omarket.trading.quote.QuoteFactory;
import rx.Observable;

import java.io.*;
import java.math.BigDecimal;
import java.math.MathContext;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;

import static org.omarket.trading.MarketData.*;

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

            try {
                processHistoricalQuotes(vertx, dirs, ibCode, new QuoteProcessor() {

                    @Override
                    public void processQuote(Quote quote) {
                        logger.info("sending: " + quote);
                        JsonObject quoteJson = QuoteConverter.toJSON(quote);
                        vertx.eventBus().send(HistoricalDataVerticle.ADDRESS_PROVIDE_HISTORY, quoteJson);
                    }

                });
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        startFuture.complete();
    }


    static public void processHistoricalQuotes(Vertx vertx, List<String> dirs, Integer ibCode, QuoteProcessor processor) throws IOException {
        String storageDirPathName = String.join(File.separator, dirs);
        Path storageDirPath = FileSystems.getDefault().getPath(storageDirPathName);
        Path productStorage = storageDirPath.resolve(createChannelQuote(ibCode));
        logger.info("accessing storage: " + productStorage);
        if (Files.exists(productStorage)) {
            Map<String, Path> tickFiles = getTickFiles(ibCode, productStorage);
            for (Map.Entry<String, Path> entry : tickFiles.entrySet()) {
                String yyyymmddhh = entry.getKey();
                Path filePath = entry.getValue();
                CSVReader reader = new CSVReader(Files.newBufferedReader(filePath, Charset.forName("US-ASCII")));
                Observable
                        .from(reader)
                        .forEach(
                                csvRow -> {
                                    Quote quote = getQuote(yyyymmddhh, csvRow);
                                    processor.processQuote(quote);
                                }
                        );
            }
        } else {
            logger.info("storage data not found: " + productStorage);
        }
    }

    private static Quote getQuote(String yyyymmddhh, String[] csvRow) {
        Quote quote = null;
        if (csvRow.length > 0) {
            LocalDateTime timestamp = LocalDateTime.parse(yyyymmddhh + ":" + csvRow[0], DATE_FORMAT);
            ZonedDateTime zonedTimestamp = ZonedDateTime.of(timestamp, ZoneOffset.UTC);
            Integer volumeBid = Integer.valueOf(csvRow[1]);
            BigDecimal priceBid = new BigDecimal(csvRow[2]);
            BigDecimal priceAsk = new BigDecimal(csvRow[3]);
            Integer volumeAsk = Integer.valueOf(csvRow[4]);
            quote = QuoteFactory.create(zonedTimestamp, volumeBid, priceBid, priceAsk, volumeAsk);
        }
        return quote;
    }


}