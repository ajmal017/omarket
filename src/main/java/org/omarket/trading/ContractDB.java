package org.omarket.trading;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Created by Christophe on 04/01/2017.
 */
public class ContractDB {

    private final static Logger logger = LoggerFactory.getLogger(ContractDB.class);

    public static void saveContract(Path contractsDirPath, JsonObject product) throws IOException {
        logger.info("saving contract: " + product);
        JsonObject contract = product.getJsonObject("m_contract");
        String primaryExchange = contract.getString("m_primaryExch");
        Integer conId = contract.getInteger("m_conid");
        String securityType = contract.getString("m_secType");
        String currency = contract.getString("m_currency");
        String fileBaseName = conId.toString();
        String fileName = fileBaseName + ".json";
        Path exchangePath = contractsDirPath.resolve(securityType).resolve(currency).resolve(primaryExchange);
        if(fileBaseName.length() < 3){
            logger.error("skipping short name: " + fileBaseName);
            return;
        }
        String initials = fileBaseName.substring(0, 3);
        Path targetPath = exchangePath.resolve(initials);
        if(Files.notExists(targetPath)){
            Files.createDirectories(targetPath);
        }
        Path filePath = targetPath.resolve(fileName);
        if(Files.notExists(filePath)){
            Files.createFile(filePath);
        }
        BufferedWriter writer = Files.newBufferedWriter(filePath, StandardCharsets.UTF_8, StandardOpenOption.TRUNCATE_EXISTING);
        GsonBuilder gsonBuilder = new GsonBuilder();
        Gson gson = gsonBuilder.create();
        gson.toJson(product.getMap(), writer);
        writer.close();
    }
}
