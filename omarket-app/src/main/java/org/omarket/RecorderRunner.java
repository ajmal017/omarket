package org.omarket;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.Objects;


@Slf4j
@Component
public class RecorderRunner implements ApplicationRunner {

    private final RecorderService service;

    @Value("${org.omarket.client_id.record_prices}")
    private String clientId;

    @Autowired
    public RecorderRunner(RecorderService service) {
        this.service = service;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        if (Objects.equals(args.getNonOptionArgs().get(0), "record-prices")) {
            service.record();
        }
    }
}
