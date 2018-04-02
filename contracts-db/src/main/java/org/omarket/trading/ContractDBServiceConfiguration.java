package org.omarket.trading;

import org.springframework.boot.SpringBootConfiguration;
import org.springframework.context.annotation.Bean;

@SpringBootConfiguration
public class ContractDBServiceConfiguration {
    @Bean
    public ContractDBService service() {
        return new ContractDBServiceImpl();
    }
}