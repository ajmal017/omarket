package org.omarket.ibroker;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.context.annotation.PropertySource;
import org.springframework.test.context.ActiveProfiles;

@Configuration
@PropertySource("classpath:test-application.properties")
@ActiveProfiles("Online tests")
public class IBrokerOnlineTestConfig {

    @Autowired
    IBrokerContractDetailsService service;

    @Value("${ibrokers.host}")
    private String ibrokersHost;

    @Value("${ibrokers.port}")
    private String ibrokersPort;

    @Value("${org.omarket.client_id.update_contracts}")
    private String ibrokersClientId;

    @Bean
    public IBrokerContractDetailsService testServiceContractDetails() throws IBrokerConnectionFailure {
        service.connect(Integer.valueOf(ibrokersClientId), ibrokersHost, Integer.valueOf(ibrokersPort));
        return service;
    }

}