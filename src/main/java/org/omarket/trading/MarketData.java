package org.omarket.trading;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.omarket.trading.verticles.MarketDataVerticle;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
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

    public static Path createIBrokersProductDescription(Path storageDirPath, JsonObject contractDetails) throws IOException {
        Integer ibCode = contractDetails.getJsonObject("m_contract").getInteger("m_conid");
        Path productStorage = storageDirPath.resolve(createChannelQuote(ibCode.toString()));
        logger.info("preparing storage for contract: " + productStorage);
        Files.createDirectories(productStorage);
        Path descriptionFilePath = productStorage.resolve("description.json");
        if(!Files.exists(descriptionFilePath)){
            Files.createFile(descriptionFilePath);
        }
        BufferedWriter writer = Files.newBufferedWriter(descriptionFilePath, StandardCharsets.UTF_8, StandardOpenOption.TRUNCATE_EXISTING);
        GsonBuilder gsonBuilder = new GsonBuilder();
        Gson gson = gsonBuilder.create();
        gson.toJson(contractDetails.getMap(), writer);
        writer.close();
        return productStorage;
    }

    public static JsonObject loadIBrokersProductDescription(Path storageDirPath, String productCode) throws IOException {
        Path productStorage = storageDirPath.resolve(createChannelQuote(productCode));
        Path descriptionFilePath = productStorage.resolve("description.json");
        BufferedReader reader = Files.newBufferedReader(descriptionFilePath, StandardCharsets.UTF_8);
        GsonBuilder gsonBuilder = new GsonBuilder();
        Gson gson = gsonBuilder.create();
        Map productMap = gson.fromJson(reader, Map.class);
        reader.close();
        return new JsonObject(productMap);
    }
}
