package org.omarket.trading.verticles;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.File;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Arrays;


/**
 * Created by Christophe on 04/01/2017.
 */
@Component
public class VerticleProperties {
    public static final String PROPERTY_IBROKERS_TICKS_PATH = "ibrokers.ticks.storagePath";
    public final static String PROPERTY_CONTRACT_DB_PATH = "oot.contracts.dbPath";

    @Value("${ibrokers.port}")
    private String ibrokersPort;

    @Value("${ibrokers.host}")
    private String ibrokersHost;

    public DeploymentOptions makeDeploymentOptions(int defaultClientId) {
        JsonArray defaultTickStoragePath = new JsonArray(Arrays.asList("data", "ticks"));
        JsonArray defaultContractDBPath = new JsonArray(Arrays.asList("data", "contracts"));
        JsonObject jsonConfig = new JsonObject()
                .put(PROPERTY_CONTRACT_DB_PATH, defaultContractDBPath)
                .put(PROPERTY_IBROKERS_TICKS_PATH, defaultTickStoragePath)
                .put("ibrokers.clientId", defaultClientId)
                .put("ibrokers.host", ibrokersHost)
                .put("ibrokers.port", Integer.valueOf(ibrokersPort))
                .put("runBacktestFlag", false);
        return new DeploymentOptions().setConfig(jsonConfig);
    }

    public static Path makePath(JsonArray values) {
        String storageDirPathName = String.join(File.separator, values.getList());
        return FileSystems.getDefault().getPath(storageDirPathName);
    }

}
