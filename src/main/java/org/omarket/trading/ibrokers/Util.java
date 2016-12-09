package org.omarket.trading.ibrokers;

import com.ib.client.EClientSocket;
import com.ib.client.EJavaSignal;
import com.ib.client.EReader;
import com.ib.client.EReaderSignal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Christophe on 08/12/2016.
 */
public class Util {

    private static Logger logger = LoggerFactory.getLogger(Util.class);

    public static void ibrokers_connect(String ibrokersHost, int ibrokersPort, int ibrokersClientId, IBrokersMarketDataCallback ewrapper) {
        final EReaderSignal readerSignal = new EJavaSignal();
        final EClientSocket clientSocket = new EClientSocket(ewrapper, readerSignal);
        ewrapper.setClient(clientSocket);
        clientSocket.eConnect(ibrokersHost, ibrokersPort, ibrokersClientId);
        if(!clientSocket.isConnected()){
            logger.error("failed to connect to IBrokers client");
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
                        logger.debug("IBrokers thread waiting for signal");
                        reader.processMsgs();
                    } catch (Exception e) {
                        logger.error("Exception", e);
                    }
                }
                if (clientSocket.isConnected()) {
                    clientSocket.eDisconnect();
                }
            }).start();
        }
    }
}