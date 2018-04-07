package org.omarket.trading.ibrokers;

import com.ib.client.EClientSocket;
import com.ib.client.EJavaSignal;
import com.ib.client.EReader;
import com.ib.client.EReaderSignal;
import lombok.extern.slf4j.Slf4j;

import static java.lang.String.format;

/**
 * Created by Christophe on 08/12/2016.
 */
@Slf4j
public class Util {

    public static EClientSocket ibrokers_connect(String ibrokersHost, int ibrokersPort, int ibrokersClientId, IBrokersMarketDataCallback ewrapper) throws IBrokersConnectionFailure {
        final EReaderSignal readerSignal = new EJavaSignal();
        final EClientSocket clientSocket = new EClientSocket(ewrapper, readerSignal);
        ewrapper.setClient(clientSocket);
        log.info("connecting to ibroker client id {} ({}:{})", ibrokersClientId, ibrokersHost, ibrokersPort);
        clientSocket.eConnect(ibrokersHost, ibrokersPort, ibrokersClientId);
        if (!clientSocket.isConnected()) {
            throw new IBrokersConnectionFailure(ibrokersHost, ibrokersPort);
        } else {
            /*
            Launching IBrokers client thread
             */
            new Thread(() -> {
                EReader reader = new EReader(clientSocket, readerSignal);
                reader.start();
                while (clientSocket.isConnected()) {
                    readerSignal.waitForSignal();
                    try {
                        log.debug("IBrokers thread waiting for signal");
                        reader.processMsgs();
                    } catch (Exception e) {
                        log.error("Exception", e);
                    }
                }
                if (clientSocket.isConnected()) {
                    clientSocket.eDisconnect();
                }
            }).start();
        }
        return clientSocket;
    }
}
