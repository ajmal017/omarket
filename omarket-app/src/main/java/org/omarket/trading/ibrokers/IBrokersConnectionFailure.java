package org.omarket.trading.ibrokers;

/**
 * Created by christophe on 12/12/16.
 */
public class IBrokersConnectionFailure extends Throwable {
    public IBrokersConnectionFailure(String ibrokersHost, int ibrokersPort) {
        super("failed to connect to IBrokers client: " + ibrokersHost + "/" + ibrokersPort);
    }
}
