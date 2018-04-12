package org.omarket.trading.ibrokers;

import lombok.extern.slf4j.Slf4j;

/**
 * Created by Christophe on 08/12/2016.
 */
@Slf4j
public class Util {

    public static void ibrokerConnect(String ibrokerHost, int ibrokerPort, int ibrokerClientId, AbstractIBrokerClient ewrapper) throws IBrokersConnectionFailure {

        //ewrapper.setClient(clientSocket);
        ewrapper.init(ibrokerClientId, ibrokerHost, ibrokerPort);
        ewrapper.startMessageProcessingThread();
    }
}
