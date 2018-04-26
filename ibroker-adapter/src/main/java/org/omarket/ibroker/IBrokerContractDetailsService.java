package org.omarket.ibroker;

import com.ib.client.Contract;

import java.io.IOException;

public interface IBrokerContractDetailsService {
    void requestContractDetails(Contract contract);

    void connect(Integer clientId, String ibrokesHost, Integer ibrokerPort) throws IBrokerConnectionFailure;

    void startMessageProcessing() throws IOException;
}
