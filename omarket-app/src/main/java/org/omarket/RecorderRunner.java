package org.omarket;

import io.vertx.core.DeploymentOptions;
import lombok.extern.slf4j.Slf4j;
import org.omarket.trading.verticles.VerticleProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.Objects;


@Slf4j
@Component
public class RecorderRunner implements ApplicationRunner {

    private final RecorderService service;

    @Autowired
    public RecorderRunner(RecorderService service) {
        this.service = service;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        if (Objects.equals(args.getNonOptionArgs().get(0), "record-prices")) {
            DeploymentOptions options;
            int defaultClientId = 3;
            Integer clientId;
            if (args.containsOption("--client-id")) {
                clientId = Integer.valueOf(args.getOptionValues("--client-id").get(0));
            } else {
                clientId = defaultClientId;
            }
            options = VerticleProperties.makeDeploymentOptions(clientId);
            service.record(options);
        }
    }
}
