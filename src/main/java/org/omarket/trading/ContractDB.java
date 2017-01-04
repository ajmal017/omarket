package org.omarket.trading;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by Christophe on 04/01/2017.
 */
public class ContractDB {

    private final static Logger logger = LoggerFactory.getLogger(ContractDB.class);
    public static ContractFilter ALL = new ContractFilter(){

        @Override
        protected boolean accept(String content) {
            return true;
        }
    };

    public abstract static class ContractFilter{

        private Path filename;
        private Path primaryExchange;
        private Path currency;
        private Path securityType;

        protected String prepare(Path path) throws IOException {
            int count = path.getNameCount();
            filename = path.getFileName();
            primaryExchange = path.getName(count - 3);
            currency = path.getName(count - 4);
            securityType = path.getName(count - 5);
            return Files.lines(path, StandardCharsets.UTF_8).collect(Collectors.joining());
        }

        public String getFilename() {
            return filename.toString();
        }

        public String getPrimaryExchange() {
            return primaryExchange.toString();
        }

        public String getCurrency() {
            return currency.toString();
        }

        public String getSecurityType() {
            return securityType.toString();
        }
        protected abstract boolean accept(String content);
    }

    public static JsonObject loadContract(Path contractsDirPath, String productCode) throws IOException {
        final Path[] targetFile = new Path[1];
        Files.walkFileTree(contractsDirPath, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                    throws IOException
            {
                if(file.getFileName().toString().equals(productCode + ".json")){
                    targetFile[0] = file;
                    return FileVisitResult.TERMINATE;
                } else {
                    return FileVisitResult.CONTINUE;
                }
            }
        });
        Path descriptionFilePath = targetFile[0];
        if(descriptionFilePath == null){
            throw new IOException("missing data for contract: " + productCode);
        }
        String content = Files.lines(descriptionFilePath, StandardCharsets.UTF_8).collect(Collectors.joining());
        return new JsonObject(content);
    }

    public static void saveContract(Path contractsDirPath, JsonObject product) throws IOException {
        JsonObject contract = product.getJsonObject("m_contract");
        String primaryExchange = contract.getString("m_primaryExch");
        Integer conId = contract.getInteger("m_conid");
        String securityType = contract.getString("m_secType");
        String currency = contract.getString("m_currency");
        String fileBaseName = conId.toString();
        String fileName = fileBaseName + ".json";
        Path exchangePath = contractsDirPath.resolve(securityType).resolve(currency).resolve(primaryExchange);
        String initials;
        if(fileBaseName.length() < 3){
            initials = fileBaseName;
        } else {
            initials = fileBaseName.substring(0, 3);
        }
        Path targetPath = exchangePath.resolve(initials);
        if(Files.notExists(targetPath)){
            Files.createDirectories(targetPath);
        }
        Path filePath = targetPath.resolve(fileName);
        if(Files.notExists(filePath)){
            Files.createFile(filePath);
        }
        BufferedWriter writer = Files.newBufferedWriter(filePath, StandardCharsets.UTF_8, StandardOpenOption.TRUNCATE_EXISTING);
        writer.write(Json.encode(product));
        writer.close();
        logger.info("saved contract: " + filePath);
    }

    public static JsonArray loadContracts(Path contractsDirPath, ContractFilter filter) throws IOException {
        JsonArray output = new JsonArray();
        Files.walkFileTree(contractsDirPath, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                    throws IOException
            {
                String content = filter.prepare(file);
                if(filter.accept(content)){
                    logger.info("processing file: " + file);
                    JsonObject contract =  new JsonObject(content);
                    output.add(contract);
                }
                return FileVisitResult.CONTINUE;
            }
        });
        return output;
    }
}
