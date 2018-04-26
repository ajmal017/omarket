package org.omarket.ibroker;

import com.ib.client.Contract;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.test.context.ActiveProfiles;

@Configuration
@PropertySource("classpath:test-application.properties")
@ActiveProfiles("Offline tests")
public class IBrokerOfflineTestConfig {

    @Bean
    public IBrokerContractDetailsService testServiceContractDetails() throws IBrokerConnectionFailure {
        return new IBrokerContractDetailsService() {

            @Override
            public void requestContractDetails(Contract contract) {

            }

            @Override
            public void connect(Integer clientId, String ibrokerHost, Integer ibrokerPort) {

            }

            @Override
            public void startMessageProcessing() {

            }
        };
    }
}
