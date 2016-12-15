package org.omarket.trading;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.omarket.trading.quote.Quote;
import org.omarket.trading.quote.QuoteFactory;
import org.omarket.trading.verticles.MarketDataVerticle;
import org.omarket.trading.verticles.QuoteProcessor;
import org.omarket.trading.verticles.StrategyProcessor;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Created by christophe on 30/11/16.
 */
public class MarketData {
    public static final String IBROKERS_TICKS_STORAGE_PATH = "ibrokers.ticks.storagePath";
    private final static Logger logger = LoggerFactory.getLogger(MarketData.class);

    static public String createChannelQuote(Integer ibCode) {
        return MarketDataVerticle.ADDRESS_ORDER_BOOK_LEVEL_ONE + "." + ibCode;
    }

    static private final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss.SSS");

    static public void processBacktest(List<String> dirs, Integer ibCode, QuoteProcessor processor) {
        String storageDirPathName = String.join(File.separator, dirs);
        Path storageDirPath = FileSystems.getDefault().getPath(storageDirPathName);
        Path productStorage = storageDirPath.resolve(createChannelQuote(ibCode));
        logger.info("accessing storage: " + productStorage);
        if (Files.exists(productStorage)) {
            Map<String, Path> tickFiles = getTickFiles(ibCode, productStorage);
            // TODO: swap for / while loops
            for (Map.Entry<String, Path> entry : tickFiles.entrySet()) {
                String yyyymmddhh = entry.getKey();
                Path filePath = entry.getValue();
                String fullLine = null;

                try (Scanner scanner = new Scanner(filePath, "utf-8")) {
                    scanner.useDelimiter("\n");
                    Quote quotePrev = null;
                    while (scanner.hasNext()) {
                        String rawLine = scanner.next().trim();
                        if (!rawLine.equals("")) {
                            fullLine = yyyymmddhh + ":" + rawLine;
                            String[] fields = fullLine.split(",");
                            LocalDateTime timestamp = LocalDateTime.parse(fields[0], DATE_FORMAT);
                            ZonedDateTime zonedTimestamp = ZonedDateTime.of(timestamp, ZoneOffset.UTC);
                            Integer volumeBid = Integer.valueOf(fields[1]);
                            BigDecimal priceBid = new BigDecimal(fields[2]);
                            BigDecimal priceAsk = new BigDecimal(fields[3]);
                            Integer volumeAsk = Integer.valueOf(fields[4]);
                            Quote quote = QuoteFactory.create(zonedTimestamp, volumeBid, priceBid, priceAsk, volumeAsk);
                            logger.debug("current quote: " + quote + " (" + quote.getLastModified() + ")");
                            if (quotePrev != null && !quote.sameSampledTime(quotePrev, ChronoUnit.SECONDS)){
                                processor.processQuote(quotePrev);
                            }
                            quotePrev = quote;
                        }
                    }
                    scanner.close();
                } catch (IOException e) {
                    logger.error("unable to access tick file: " + filePath, e);
                }
            }
        } else {
            logger.info("storage data not found: " + productStorage);
        }
    }

    /**
     * Detects tick files from local drive.
     *
     * @param ibCode
     * @param productStorage
     * @return
     */
    private static Map<String, Path> getTickFiles(Integer ibCode, Path productStorage) {
        Map<String, Path> tickFiles = new TreeMap<>();
        try (Stream<Path> paths = Files.walk(productStorage)) {
            paths.forEach(filePath -> {
                if (Files.isRegularFile(filePath)) {
                    logger.debug("regular file detected: " + filePath.toAbsolutePath());
                    Pattern yyyymmddhhURIEnding = Pattern.compile(".*([0-9]{8})\\/([0-9]{2})$");
                    Matcher matcher = yyyymmddhhURIEnding.matcher(filePath.toUri().toString());
                    if (matcher.matches()) {
                        String yyyymmddhh = matcher.group(1) + " " + matcher.group(2);
                        logger.debug("will be processing recorded ticks: " + yyyymmddhh);
                        tickFiles.put(yyyymmddhh, filePath);
                    }
                }
            });
        } catch (IOException e) {
            logger.error("failed to access recorded ticks for product " + ibCode, e);
        }
        return tickFiles;
    }
}
