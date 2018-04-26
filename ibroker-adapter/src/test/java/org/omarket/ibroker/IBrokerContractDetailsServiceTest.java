package org.omarket.ibroker;

import com.ib.client.Contract;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {IBrokerTestConfig.class})
public class IBrokerContractDetailsServiceTest {

    @Autowired
    private IBrokerContractDetailsService contractDetailsService;

    @Test
    public void testRequestContractDetails() throws IBrokerConnectionFailure, IOException {
        contractDetailsService.connect(100, "127.0.0.1", 4003);
        contractDetailsService.startMessageProcessing();
        Contract contract = new Contract();
        contract.conid(130806085);
        contractDetailsService.requestContractDetails(contract);
    }

}
