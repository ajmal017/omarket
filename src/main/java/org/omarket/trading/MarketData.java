package org.omarket.trading;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.omarket.trading.verticles.MarketDataVerticle;
import org.omarket.trading.verticles.StrategyProcessor;

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

/**
 * Created by christophe on 30/11/16.
 */
public class MarketData {
    private final static Logger logger = LoggerFactory.getLogger(MarketData.class);

    static public String createChannelOrderBookLevelOne(Integer ibCode) {
        return MarketDataVerticle.ADDRESS_ORDER_BOOK_LEVEL_ONE + "." + ibCode;
    }

    private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMdd hh:mm:ss.SSS");
    static {
        DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    static public void processBacktest(List<String> dirs, Integer ibCode, StrategyProcessor processor) {
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
            // TODO: swap for / while loops
            for (Map.Entry<String, Path> entry : tickFiles.entrySet()) {
                String yyyymmddhh = entry.getKey();
                Path filePath = entry.getValue();
                String fullLine = null;

                try (Scanner scanner = new Scanner(filePath, "utf-8")) {
                    scanner.useDelimiter("\n");
                    OrderBookLevelOneImmutable orderBookPrev = null;
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
                            OrderBookLevelOneImmutable orderBook = new OrderBookLevelOneImmutable(timestamp, volumeBid, priceBid, priceAsk, volumeAsk);
                            logger.debug("current order book: " + orderBook + " (" + isoFormat.format(orderBook.getLastModified()) + ")");
                            if (orderBookPrev != null && !orderBook.sameSampledTime(orderBookPrev, OrderBookLevelOneImmutable.Sampling.SECOND)){
                                processor.processOrderBook(orderBookPrev, true);
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
}
