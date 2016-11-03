package org.omarket.trading.ibrokers;

import com.ib.client.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Thread.sleep;

/**
 * Created by Christophe on 26/09/2016.
 */
public class MarketDataMain {
    private static Logger logger = LoggerFactory.getLogger(ContractDetailsIBrokersCallback.class);

    public static void main(String[] args) throws InterruptedException {
        logger.info("starting market data");
        EReaderSignal readerSignal = new EJavaSignal();
        ContractDetailsIBrokersCallback ewrapper = new ContractDetailsIBrokersCallback();
        EClientSocket clientSocket = new EClientSocket(ewrapper, readerSignal);
        ewrapper.setClient(clientSocket);
        clientSocket.eConnect("127.0.0.1", 7497, 1);

        /*
        Launching IBrokers client thread
         */
        new Thread() {
            public void run() {
                EReader reader = new EReader(clientSocket, readerSignal);
                reader.start();
                while (clientSocket.isConnected()) {
                    readerSignal.waitForSignal();
                    try {
                        logger.info("IBrokers thread waiting for signal");
                        reader.processMsgs();
                    } catch (Exception e) {
                        logger.error("Exception", e);
                    }
                }
                if(clientSocket.isConnected()){
                    clientSocket.eDisconnect();
                }
            }
        }.start();

        int counter = 3;
        do {
            clientSocket.reqCurrentTime();
            sleep(1000);
        } while (--counter > 0);

        String[][] stocks = {
                {"IBM", "SMART", "USD"},
                {"MSFT", "SMART", "USD"}
        }
        ;
        for(String[] stockData: stocks) {
            Contract contract = new Contract();
            contract.symbol(stockData[0]);
            contract.secType(Types.SecType.STK.name());
            contract.exchange(stockData[1]);
            contract.currency(stockData[2]);
            ewrapper.addRequest(contract);
        }
        ewrapper.processRequests();
        logger.info("completed");
    }
}
