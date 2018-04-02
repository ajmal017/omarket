package org.omarket.eod;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URISyntaxException;

@Slf4j
@Component
class UpdateEODRunner implements CommandLineRunner {

    private final UpdateEODService service;

    @Autowired
    public UpdateEODRunner(UpdateEODService service) {
        this.service = service;
    }

    @Override
    public void run(String... args) throws Exception {
        service.update();
    }
}

@Slf4j
@SpringBootApplication
public class UpdateEODMain {

    public static void main(String[] args) throws InterruptedException, IOException, URISyntaxException {
        SpringApplication.run(UpdateEODMain.class, args);
    }

}
