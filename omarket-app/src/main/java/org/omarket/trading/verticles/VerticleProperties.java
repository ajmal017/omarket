package org.omarket.trading.verticles;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

import static java.text.MessageFormat.format;


/**
 * Created by Christophe on 04/01/2017.
 */
public class VerticleProperties {
    public static final String PROPERTY_IBROKERS_TICKS_PATH = "ibrokers.ticks.storagePath";
    public final static String PROPERTY_CONTRACT_DB_PATH = "oot.contracts.dbPath";

    public static DeploymentOptions makeDeploymentOptions(int defaultClientId) {
        JsonArray defaultTickStoragePath = new JsonArray(Arrays.asList("data", "ticks"));
        JsonArray defaultContractDBPath = new JsonArray(Arrays.asList("data", "contracts"));
        String defaultHost = "127.0.0.1";
        int defaultPort = 7497;
        JsonObject jsonConfig = new JsonObject()
                .put(PROPERTY_CONTRACT_DB_PATH, defaultContractDBPath)
                .put(PROPERTY_IBROKERS_TICKS_PATH, defaultTickStoragePath)
                .put("ibrokers.clientId", defaultClientId)
                .put("ibrokers.host", defaultHost)
                .put("ibrokers.port", defaultPort)
                .put("runBacktestFlag", false);
        return new DeploymentOptions().setConfig(jsonConfig);
    }

    public static Path makePath(JsonArray values) {
        String storageDirPathName = String.join(File.separator, values.getList());
        return FileSystems.getDefault().getPath(storageDirPathName);
    }

    public static DeploymentOptions makeDeploymentOptions(String configPath) throws IOException {
        Path configValidatedPath = Paths.get(configPath).toAbsolutePath();
        if(Files.isRegularFile(configValidatedPath)) {
            JsonParser parser = new JsonParser();
            JsonElement jsonElement = parser.parse(Files.newBufferedReader(configValidatedPath));
            com.google.gson.JsonObject googleJsonObject = jsonElement.getAsJsonObject();
            JsonObject jsonConfig = new JsonObject(googleJsonObject.toString());
            return new DeploymentOptions().setConfig(jsonConfig);
        } else {
            throw new RuntimeException(format("failed to load config file {}", configValidatedPath));
        }
    }
}
