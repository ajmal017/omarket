package org.omarket;

import io.vertx.core.DeploymentOptions;
import lombok.extern.slf4j.Slf4j;
import org.omarket.trading.verticles.VerticleProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.stereotype.Component;

import java.util.Objects;


@Slf4j
@Component
public class UpdateContractDBRunner implements ApplicationRunner {

    private final UpdateContractDBService service;

    @Value("${org.omarket.client_id.update_contracts}")
    private String clientId;

    @Autowired
    public UpdateContractDBRunner(UpdateContractDBService service) {
        this.service = service;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        if(Objects.equals(args.getNonOptionArgs().get(0), "update-contracts")) {
            DeploymentOptions options = VerticleProperties.makeDeploymentOptions(Integer.valueOf(clientId));
            service.update(options);
        }
    }
}
