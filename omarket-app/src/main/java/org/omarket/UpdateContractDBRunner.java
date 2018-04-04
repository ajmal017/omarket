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
    private final VerticleProperties props;

    @Value("${org.omarket.client_id.update_contracts}")
    private String clientId;

    @Autowired
    public UpdateContractDBRunner(UpdateContractDBService service, VerticleProperties props) {
        this.service = service;
        this.props = props;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        if(Objects.equals(args.getNonOptionArgs().get(0), "update-contracts")) {
            DeploymentOptions options = props.makeDeploymentOptions(Integer.valueOf(clientId));
            service.update(options);
        }
    }
}
