package org.omarket.trading.ibroker;

/**
 * Created by christophe on 12/12/16.
 */
public class IBrokerConnectionFailure extends Throwable {
    public IBrokerConnectionFailure(String ibrokersHost, int ibrokersPort) {
        super("connection to IBrokers client failed: " + ibrokersHost + "/" + ibrokersPort);
    }
    public IBrokerConnectionFailure(String ibrokersHost, int ibrokersPort, String errorMessage) {
        super("failed to initiate connection to IBrokers client " + ibrokersHost + "/" + ibrokersPort + ": " + errorMessage);
    }
}
