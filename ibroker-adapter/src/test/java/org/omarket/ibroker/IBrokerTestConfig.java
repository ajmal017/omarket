package org.omarket.ibroker;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
@PropertySource("classpath:test-application.properties")
public class IBrokerTestConfig {

    @Value("${ibrokers.host}")
    private String ibrokersHost;

    @Value("${ibrokers.port}")
    private String ibrokersPort;

    @Value("${org.omarket.client_id.update_contracts}")
    private String ibrokersClientId;

    @Bean
    public IBrokerContractDetailsService testServiceContractDetails() throws IBrokerConnectionFailure {
        IBrokerContractDetailsService service = new IBrokerContractDetailsService();
        service.connect(Integer.valueOf(ibrokersClientId), ibrokersHost, Integer.valueOf(ibrokersPort));
        return service;
    }
}
