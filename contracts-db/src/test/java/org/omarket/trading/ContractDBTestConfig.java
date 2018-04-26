package org.omarket.trading;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
@PropertySource("classpath:test-application.properties")
public class ContractDBTestConfig {

    @Value("${ibrokers.host}")
    private String ibrokersHost;

    @Value("${ibrokers.port}")
    private String ibrokersPort;

    @Value("${org.omarket.client_id.update_contracts}")
    private String ibrokersClientId;

    @Bean
    public ContractDBService testServiceContractService() {
        return new ContractDBServiceImpl();
    }
}
