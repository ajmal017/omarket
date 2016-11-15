package org.omarket.trading.verticles;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.omarket.trading.OrderBookLevelOne;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static java.lang.Thread.sleep;
import static org.omarket.trading.verticles.MarketDataVerticle.IBROKERS_TICKS_STORAGE_PATH;
import static org.omarket.trading.verticles.MarketDataVerticle.createChannelOrderBookLevelOne;


/**
 * Created by Christophe on 01/11/2016.
 */
public class SingleLegMeanReversionStrategyVerticle extends AbstractVerticle {
    private final static Logger logger = LoggerFactory.getLogger(SingleLegMeanReversionStrategyVerticle.class);
    private static JsonObject contract;
    private static OrderBookLevelOne orderBook;
    public static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMdd hh:mm:ss.SSS");

    static {
        DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    private static void processRecordedTicks(List<String> dirs, Integer ibCode) {
        String storageDirPathName = String.join(File.separator, dirs);
        Path storageDirPath = FileSystems.getDefault().getPath(storageDirPathName);
        Path productStorage = storageDirPath.resolve(createChannelOrderBookLevelOne(ibCode));
        logger.info("accessing storage: " + productStorage);
        if (Files.exists(productStorage)) {
            Map<String, Path> tickFiles = new TreeMap<>();
            try (Stream<Path> paths = Files.walk(productStorage)) {
                paths.forEach(filePath -> {
                    if (Files.isRegularFile(filePath)) {
                        Pattern yyyymmddhhEnding = Pattern.compile(".*([0-9]{8})\\/([0-9]{2})$");
                        Matcher matcher = yyyymmddhhEnding.matcher(filePath.toString());
                        if (matcher.matches()) {
                            String yyyymmddhh = matcher.group(1) + " " + matcher.group(2);
                            logger.info("will be processing recorded ticks: " + yyyymmddhh);
                            tickFiles.put(yyyymmddhh, filePath);
                        }
                    }
                });
            } catch (IOException e) {
                logger.error("failed to access recorded ticks for product " + ibCode, e);
            }
            for (Map.Entry<String, Path> entry : tickFiles.entrySet()) {
                String yyyymmddhh = entry.getKey();
                Path filePath = entry.getValue();
                String fullLine = null;
                try (Scanner scanner = new Scanner(filePath, "utf-8")) {
                    scanner.useDelimiter("\n");
                    while (scanner.hasNext()) {
                        String rawLine = scanner.next().trim();
                        if (!rawLine.equals("")) {
                            fullLine = yyyymmddhh + ":" + rawLine;
                            String[] fields = fullLine.split(",");
                            Date timestamp = DATE_FORMAT.parse(fields[0] + "");
                            Integer volumeBid = Integer.valueOf(fields[1]);
                            BigDecimal priceBid = new BigDecimal(fields[2]);
                            BigDecimal priceAsk = new BigDecimal(fields[3]);
                            Integer volumeAsk = Integer.valueOf(fields[4]);
                            //logger.info("line: " + timestamp + "," + volumeBid + "," + priceBid);
                        }
                    }
                    scanner.close();
                } catch (IOException e) {
                    logger.error("unable to access tick file: " + filePath, e);
                } catch (ParseException e) {
                    logger.error("unable to parse line: " + fullLine, e);
                }
            }
        }
    }

    public void start() {
        logger.info("starting single leg mean reversion strategy verticle");
        final Integer ibCode = 12087817;

        vertx.executeBlocking(future -> {
            List<String> dirs = config().getJsonArray(IBROKERS_TICKS_STORAGE_PATH).getList();
            processRecordedTicks(dirs, ibCode);
            future.succeeded();
        }, result -> {
            logger.info("processed recorded ticks for " + ibCode);
            // now launching realtime ticks
            MarketDataVerticle.subscribeProduct(vertx, ibCode, subscribeProductResult -> {
                if (subscribeProductResult.succeeded()) {
                    vertx.setPeriodic(1000, id -> {
                        // Sampling: calculates signal
                        contract = subscribeProductResult.result().body();
                        String symbol = contract.getJsonObject("m_contract").getString("m_localSymbol");
                    });

                    // Constantly maintains order book up to date
                    final String channelProduct = createChannelOrderBookLevelOne(ibCode);
                    final double minTick = contract.getDouble("m_minTick");
                    vertx.eventBus().consumer(channelProduct, (Message<JsonObject> message) -> {
                        try {
                            SingleLegMeanReversionStrategyVerticle.orderBook = OrderBookLevelOne.fromJSON(message.body(), minTick);
                        } catch (ParseException e) {
                            logger.error("failed to parse tick data for contract " + contract, e);
                        }
                    });

                } else {
                    logger.error("failed to subscribe to: " + ibCode);
                }
            });
        });

    }

}
