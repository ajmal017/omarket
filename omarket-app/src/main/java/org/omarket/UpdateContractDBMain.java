package org.omarket;

import io.vertx.core.DeploymentOptions;
import lombok.extern.slf4j.Slf4j;
import org.omarket.trading.verticles.VerticleProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Slf4j
@Component
class UpdateContractDBRunner implements CommandLineRunner {

    private final UpdateContractDBService service;

    @Autowired
    public UpdateContractDBRunner(UpdateContractDBService service) {
        this.service = service;
    }

    @Override
    public void run(String... args) throws Exception {
        DeploymentOptions options;
        if(args.length == 0){
            int defaultClientId = 2;
            options = VerticleProperties.makeDeploymentOptions(defaultClientId);
        } else {
            options = VerticleProperties.makeDeploymentOptions(args[0]);
        }
        service.update(options);
    }
}


@SpringBootApplication
@Slf4j
public class UpdateContractDBMain {

    public static void main(String[] args) throws IOException {
        SpringApplication.run(UpdateContractDBMain.class, args);
    }

}
