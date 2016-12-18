package org.omarket.trading.verticles;

import com.opencsv.CSVReader;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.eventbus.MessageConsumer;
import org.omarket.trading.quote.Quote;
import org.omarket.trading.quote.QuoteConverter;
import org.omarket.trading.quote.QuoteFactory;
import org.omarket.trading.util.OperatorMergeSorted;
import rx.Observable;

import java.io.*;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.*;

import static org.omarket.trading.MarketData.*;
import static rx.Observable.*;

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

        final MessageConsumer<JsonObject> provideRequest = vertx.eventBus().consumer(ADDRESS_PROVIDE_HISTORY);
        Observable<JsonObject> contractStream = provideRequest.bodyStream().toObservable();
        contractStream
                .subscribe(message -> {
                    final JsonArray productCodes = message.getJsonArray("productCodes");
                    final String address = message.getString("replyTo");
                    logger.info("data for contracts " + productCodes.toString() + " will be sent to " + address);
                    List<Observable<Quote>> quoteStreams = new LinkedList<>();
                    try {
                        for (Object productCode : productCodes.getList()) {
                            Observable<Quote> stream = getHistoricalQuoteStream(dirs, (String) productCode);
                            quoteStreams.add(stream);
                        }
                    } catch (IOException e) {
                        logger.error("failed while accessing historical data", e);
                        startFuture.fail(e);
                    }
                    mergeQuoteStreams(quoteStreams)
                            .forEach(
                                    quote -> {
                                        logger.debug("sending: " + quote + " on address " + address);
                                        JsonObject quoteJson = QuoteConverter.toJSON(quote);
                                        vertx.eventBus().send(address, quoteJson);
                                    },
                                    error -> {
                                        logger.error("failed to send historical data", error);
                                    },
                                    () -> {
                                        logger.info("completed historical data");
                                        JsonObject empty = new JsonObject();
                                        vertx.eventBus().send(address, empty);
                                    }
                                    );
                });
        logger.info("ready to provide historical data upon request (address: " + ADDRESS_PROVIDE_HISTORY + ")");
        startFuture.complete();
    }

    static public Observable<Quote> getHistoricalQuoteStream(final List<String> dirs, final String productCode) throws IOException {
        String storageDirPathName = String.join(File.separator, dirs);
        Path storageDirPath = FileSystems.getDefault().getPath(storageDirPathName);
        Path productStorage = storageDirPath.resolve(createChannelQuote(productCode));
        logger.info("accessing storage: " + productStorage);
        Observable<String[]> quotesStream = empty();
        if (Files.exists(productStorage)) {
            Map<String, Path> tickFiles = getTickFiles(productCode, productStorage);
            for (Map.Entry<String, Path> entry : tickFiles.entrySet()) {
                String yyyymmddhh = entry.getKey();
                Path filePath = entry.getValue();
                CSVReader reader = new CSVReader(Files.newBufferedReader(filePath, Charset.forName("US-ASCII")));
                quotesStream = quotesStream.concatWith(Observable.from(reader).map(row -> {
                    row[0] = yyyymmddhh + ":" + row[0];
                    return row;
                }));
            }
        } else {
            logger.info("storage data not found: " + productStorage);
        }
        return quotesStream.map(row -> createQuote(row, productCode));
    }

    private static Quote createQuote(String[] fields, String productCode) {
        Quote quote = null;
        if (fields.length > 0) {
            LocalDateTime timestamp = LocalDateTime.parse(fields[0], DATE_FORMAT);
            ZonedDateTime zonedTimestamp = ZonedDateTime.of(timestamp, ZoneOffset.UTC);
            Integer volumeBid = Integer.valueOf(fields[1]);
            BigDecimal priceBid = new BigDecimal(fields[2]);
            BigDecimal priceAsk = new BigDecimal(fields[3]);
            Integer volumeAsk = Integer.valueOf(fields[4]);
            quote = QuoteFactory.create(zonedTimestamp, volumeBid, priceBid, priceAsk, volumeAsk, productCode);
        }
        return quote;
    }

    public static Observable<Quote> mergeQuoteStreams(List<Observable<Quote>> quoteStreams) {
        return Observable.from(quoteStreams)
                .lift(new OperatorMergeSorted<>((x, y) -> {
                    if (x.getLastModified().equals(y.getLastModified())) {
                        return 0;
                    } else if (x.getLastModified().isBefore(y.getLastModified())) {
                        return -1;
                    } else {
                        return 1;
                    }
                }));
    }

}