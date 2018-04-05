package org.omarket.trading.verticles;

import com.opencsv.CSVReader;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.eventbus.MessageConsumer;
import lombok.extern.slf4j.Slf4j;
import org.omarket.trading.quote.Quote;
import org.omarket.trading.quote.QuoteConverter;
import org.omarket.trading.quote.QuoteFactory;
import org.omarket.trading.util.OperatorMergeSorted;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import rx.Observable;

import java.io.*;
import java.math.BigDecimal;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.*;

import static org.omarket.trading.MarketData.*;
import static rx.Observable.*;

/**
 * Created by Christophe on 01/11/2016.
 */
@Slf4j
@Component
public class HistoricalDataVerticle extends AbstractVerticle {
    public final static String ADDRESS_PROVIDE_HISTORY = "oot.historicalData.provide";

    @Value("${ibrokers.ticks.storagePath}")
    private String storageDir;

    private final QuoteFactory quoteFactory;

    @Autowired
    public HistoricalDataVerticle(QuoteFactory quoteFactory) {
        this.quoteFactory = quoteFactory;
    }

    public void start(Future<Void> startFuture) throws Exception {
        log.info("starting historical data");
        Path storageDirPath = Paths.get(storageDir).toAbsolutePath();
        log.info("ticks data storage set to '" + storageDirPath + "'");

        final MessageConsumer<JsonObject> provideRequest = vertx.eventBus().consumer(ADDRESS_PROVIDE_HISTORY);
        Observable<JsonObject> contractStream = provideRequest.bodyStream().toObservable();
        contractStream
                .subscribe(message -> {
                    Observable<Object> execStream = vertx.executeBlockingObservable(future -> {
                        final JsonArray productCodes = message.getJsonArray(AbstractStrategyVerticle.KEY_PRODUCT_CODES);
                        final String address = message.getString(AbstractStrategyVerticle.KEY_REPLY_TO);
                        final String completionAddress = message.getString(AbstractStrategyVerticle.KEY_COMPLETION_ADDRESS);
                        log.info("data for contracts " + productCodes.toString() + " will be sent to " + address);
                        List<Observable<Quote>> quoteStreams = new LinkedList<>();
                        try {
                            for (Object productCode : productCodes.getList()) {
                                Observable<Quote> stream = getHistoricalQuoteStream(storageDirPath, (String) productCode);
                                quoteStreams.add(stream);
                            }
                        } catch (IOException e) {
                            log.error("failed while accessing historical data", e);
                            future.fail(e);
                        }
                        mergeQuoteStreams(quoteStreams)
                                .map(QuoteConverter::toJSON)
                                .forEach(
                                        quoteJson -> {
                                            log.debug("sending: " + quoteJson + " on address " + address);
                                            vertx.eventBus().send(address, quoteJson);
                                        },
                                        future::fail,
                                        () -> {
                                            JsonObject empty = new JsonObject();
                                            log.info("notifying completion on " + completionAddress);
                                            vertx.eventBus().send(completionAddress, empty);
                                            future.complete();
                                        }
                                );
                    });
                    execStream
                            .doOnCompleted(() -> {
                                log.info("completed historical data");
                            })
                            .doOnError(error -> {
                                log.error("failed to send historical data", error);
                            })
                            .subscribe();
                });
        log.info("ready to provide historical data upon request (address: " + ADDRESS_PROVIDE_HISTORY + ")");
        startFuture.complete();
    }

    public Observable<Quote> getHistoricalQuoteStream(final Path storageDirPath, final String productCode) throws IOException {
        Path productStorage = storageDirPath.resolve(createChannelQuote(productCode));
        log.info("accessing storage: " + productStorage);
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
            log.info("storage data not found: " + productStorage);
        }
        return quotesStream.map(row -> createQuote(row, productCode));
    }

    private Quote createQuote(String[] fields, String productCode) {
        Quote quote = null;
        if (fields.length > 0) {
            LocalDateTime timestamp = LocalDateTime.parse(fields[0], DATE_FORMAT);
            ZonedDateTime zonedTimestamp = ZonedDateTime.of(timestamp, ZoneOffset.UTC);
            Integer volumeBid = Integer.valueOf(fields[1]);
            BigDecimal priceBid = new BigDecimal(fields[2]);
            BigDecimal priceAsk = new BigDecimal(fields[3]);
            Integer volumeAsk = Integer.valueOf(fields[4]);
            quote = quoteFactory.create(zonedTimestamp, volumeBid, priceBid, priceAsk, volumeAsk, productCode);
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