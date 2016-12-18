package org.omarket.trading;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.omarket.trading.verticles.MarketDataVerticle;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.format.DateTimeFormatter;
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

    static public String createChannelQuote(String productCode) {
        return MarketDataVerticle.ADDRESS_ORDER_BOOK_LEVEL_ONE + "." + productCode;
    }

    public static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss.SSS");

    /**
     * Detects tick files from local drive.
     *
     * @param productCode
     * @param productStorage
     * @return
     */
    public static Map<String, Path> getTickFiles(String productCode, Path productStorage) {
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
            logger.error("failed to access recorded ticks for product " + productCode, e);
        }
        return tickFiles;
    }
}
