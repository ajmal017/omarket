package org.omarket.trading.verticles;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.omarket.trading.OrderBookLevelOneImmutable;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.omarket.trading.verticles.MarketDataVerticle.IBROKERS_TICKS_STORAGE_PATH;
import static org.omarket.trading.verticles.MarketDataVerticle.createChannelOrderBookLevelOne;

/**
 * Created by Christophe on 18/11/2016.
 */

abstract class AbstractStrategyVerticle extends AbstractVerticle {
    private final static Logger logger = LoggerFactory.getLogger(AbstractStrategyVerticle.class);
    private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMdd hh:mm:ss.SSS");
    static {
        DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    private OrderBookLevelOneImmutable orderBookPrev;
    private OrderBookLevelOneImmutable orderBook;
    private Map<Integer, JsonObject> contracts = new HashMap<>();
    protected abstract void processOrderBook(OrderBookLevelOneImmutable orderBook, boolean isBacktest);

    abstract protected Integer[] getIBrokersCodes();
    abstract protected void init();

    private void processBacktest(List<String> dirs, Integer ibCode) {
        String storageDirPathName = String.join(File.separator, dirs);
        Path storageDirPath = FileSystems.getDefault().getPath(storageDirPathName);
        Path productStorage = storageDirPath.resolve(createChannelOrderBookLevelOne(ibCode));
        logger.info("accessing storage: " + productStorage);
        if (Files.exists(productStorage)) {
            Map<String, Path> tickFiles = new TreeMap<>();
            try (Stream<Path> paths = Files.walk(productStorage)) {
                paths.forEach(filePath -> {
                    if (Files.isRegularFile(filePath)) {
                        logger.info("regular file detected: " + filePath.toAbsolutePath());
                        Pattern yyyymmddhhURIEnding = Pattern.compile(".*([0-9]{8})\\/([0-9]{2})$");
                        Matcher matcher = yyyymmddhhURIEnding.matcher(filePath.toUri().toString());
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
                            DateFormat isoFormat = new SimpleDateFormat("yyyyMMdd hh:mm:ss.SSS");
                            orderBook = new OrderBookLevelOneImmutable(timestamp, volumeBid, priceBid, priceAsk, volumeAsk);
                            logger.debug("current order book: " + orderBook + " (" + isoFormat.format(orderBook.getLastModified()) + ")");
                            if (orderBookPrev != null && !orderBook.sameSampledTime(orderBookPrev, OrderBookLevelOneImmutable.Sampling.SECOND)){
                                processOrderBook(orderBookPrev, true);
                            }
                            orderBookPrev = orderBook;
                        }
                    }
                    scanner.close();
                } catch (IOException e) {
                    logger.error("unable to access tick file: " + filePath, e);
                } catch (ParseException e) {
                    logger.error("unable to parse line: " + fullLine, e);
                }
            }
        } else {
            logger.info("Storage data not found: " + productStorage);
        }
    }

    @Override
    public void start() {
        init();
        for (Integer ibCode: getIBrokersCodes()) {
            vertx.executeBlocking(future -> {
                JsonArray storageDirs = config().getJsonArray(IBROKERS_TICKS_STORAGE_PATH);
                List<String> dirs = storageDirs.getList();
                try {
                    processBacktest(dirs, ibCode);
                    future.complete();
                } catch (Exception e) {
                    logger.error("backtest failed", e);
                    future.fail(e);
                }
            }, result -> {
                logger.info("processed recorded ticks for " + ibCode);
                // now launching realtime ticks
                MarketDataVerticle.subscribeProduct(vertx, ibCode, subscribeProductResult -> {
                    if (subscribeProductResult.succeeded()) {
                        JsonObject contract = subscribeProductResult.result().body();
                        contracts.put(ibCode, contract);
                        // Constantly maintains order book up to date
                        final String channelProduct = createChannelOrderBookLevelOne(ibCode);
                        vertx.eventBus().consumer(channelProduct, (Message<JsonObject> message) -> {
                            try {
                                orderBook = OrderBookLevelOneImmutable.fromJSON(message.body());
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

        vertx.setPeriodic(1000, id -> {
            if(contracts.size() != getIBrokersCodes().length){
                return;
            }
            // Sampling: calculates signal
            logger.info("processing order book: " + orderBook);
            processOrderBook(orderBook, false);
        });
    }
}

