package org.omarket.trading.verticles;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.Arrays;

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
}
