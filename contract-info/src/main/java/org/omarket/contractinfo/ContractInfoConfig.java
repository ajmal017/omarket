package org.omarket.contractinfo;

import org.omarket.ibroker.IBrokerConnectionFailure;
import org.omarket.ibroker.IBrokerContractDetailsService;
import org.omarket.trading.ContractDBService;
import org.omarket.trading.ContractDBServiceImpl;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.PropertySource;

@SpringBootConfiguration
public class ContractInfoConfig {

    @Value("${ibrokers.host}")
    private String ibrokersHost;

    @Value("${ibrokers.port}")
    private String ibrokersPort;

    @Value("${org.omarket.client_id.update_contracts}")
    private String ibrokersClientId;

    @Bean
    public IBrokerContractDetailsService serviceContractDetails() throws IBrokerConnectionFailure {
        IBrokerContractDetailsService service = new IBrokerContractDetailsService();
        service.connect(Integer.valueOf(ibrokersClientId), ibrokersHost, Integer.valueOf(ibrokersPort));
        return service;
    }

    @Bean
    public ContractDBService service() {
        return new ContractDBServiceImpl();
    }
}
