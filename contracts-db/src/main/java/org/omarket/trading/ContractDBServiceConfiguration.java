package org.omarket.trading;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@TestConfiguration
public class ContractDBServiceConfiguration {
    @Bean
    public ContractDBService contractDBService() {
        return new ContractDBServiceImpl();
    }
}
