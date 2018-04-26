package org.omarket.ibroker;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Created by christophe on 30/11/16.
 */
@Slf4j
@Component
public class MarketData {

    public static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss.SSS");
    @Value("${address.order_book_level_one}")
    private String ADDRESS_ORDER_BOOK_LEVEL_ONE;

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
                    log.debug("regular file detected: " + filePath.toAbsolutePath());
                    Pattern yyyymmddhhURIEnding = Pattern.compile(".*([0-9]{8})\\/([0-9]{2})$");
                    Matcher matcher = yyyymmddhhURIEnding.matcher(filePath.toUri().toString());
                    if (matcher.matches()) {
                        String yyyymmddhh = matcher.group(1) + " " + matcher.group(2);
                        log.debug("will be processing recorded ticks: " + yyyymmddhh);
                        tickFiles.put(yyyymmddhh, filePath);
                    }
                }
            });
        } catch (IOException e) {
            log.error("failed to access recorded ticks for product " + productCode, e);
        }
        return tickFiles;
    }

    public String createChannelQuote(String productCode) {
        return ADDRESS_ORDER_BOOK_LEVEL_ONE + "." + productCode;
    }

}
