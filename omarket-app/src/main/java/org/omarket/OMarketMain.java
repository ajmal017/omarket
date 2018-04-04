package org.omarket;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import java.io.IOException;

@Slf4j
@SpringBootApplication
@ComponentScan({"org.omarket"})
public class OMarketMain {

    public static void main(String[] args) throws IOException {
        SpringApplication.run(OMarketMain.class, args);
    }

}
