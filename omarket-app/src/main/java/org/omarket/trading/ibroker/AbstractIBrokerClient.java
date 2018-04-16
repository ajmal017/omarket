package org.omarket.trading.ibroker;

import com.ib.client.CommissionReport;
import com.ib.client.Contract;
import com.ib.client.ContractDetails;
import com.ib.client.DeltaNeutralContract;
import com.ib.client.EClient;
import com.ib.client.EClientSocket;
import com.ib.client.EJavaSignal;
import com.ib.client.EReader;
import com.ib.client.EReaderSignal;
import com.ib.client.EWrapper;
import com.ib.client.Execution;
import com.ib.client.Order;
import com.ib.client.OrderState;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;

import static java.lang.String.format;
import static java.lang.Thread.sleep;

/**
 * Created by Christophe on 01/11/2016.
 */
@Slf4j
public abstract class AbstractIBrokerClient implements EWrapper {

    public static final int IBROKER_TIMEOUT_MILLIS = 10000;
    private Integer lastRequestId = null;
    private EClientSocket clientSocket;
    final private EReaderSignal readerSignal = new EJavaSignal();
    private ConcurrentLinkedQueue<Pair<Integer, String>> errors = new ConcurrentLinkedQueue<>();

    public EClient getClientSocket() {
        return this.clientSocket;
    }

    /**
     * Connects to IBroker TWS or Gateway
     *
     * @param ibrokerClientId
     * @param ibrokerHost
     * @param ibrokerPort
     * @throws IBrokerConnectionFailure
     */
    public void connect(int ibrokerClientId, String ibrokerHost, int ibrokerPort) throws IBrokerConnectionFailure {
        final AbstractIBrokerClient ibClientRef = this;
        final Thread ibConnect;
        ibConnect = new Thread() {
            public void run(){
                            clientSocket = new EClientSocket(ibClientRef, readerSignal);
                            log.info("connecting to ibroker client id {} ({}:{})", ibrokerClientId, ibrokerHost, ibrokerPort);
                            clientSocket.eConnect(ibrokerHost, ibrokerPort, ibrokerClientId);
            }
        };
        ibConnect.start();
        try {
            sleep(IBROKER_TIMEOUT_MILLIS);
            if (!this.clientSocket.isConnected()) {
                ibConnect.interrupt();
                throw new IBrokerConnectionFailure(ibrokerHost, ibrokerPort);
            }
        } catch (InterruptedException e) {
            log.error("interruption while connecting to IBroker");
        }
    }

    /**
     * Launching IBroker API client thread.
     *
     * Looping on received signals and blocking until disconnect.
     */
    public void startMessageProcessing() throws IOException {
        final EClientSocket clientSocket = this.clientSocket;
        EReader reader = new EReader(clientSocket, readerSignal);
        reader.start();
        Thread ibrokerSignalProcessor = new Thread(() -> {
            while (clientSocket.isConnected()) {
                log.debug("IBrokers thread waiting for signal");
                readerSignal.waitForSignal();
                try {
                    reader.processMsgs();
                } catch (IOException e) {
                    log.error("failed to process signal from IBroker", e);
                }
            }
            if (clientSocket.isConnected()) {
                clientSocket.eDisconnect();
            }
        });
        ibrokerSignalProcessor.start();
    }

    /**
     * Generates a new valid request Id.
     *
     * @return
     */
    synchronized protected Integer newRequestId() {
        if (lastRequestId == null) {
            lastRequestId = 0;
        }
        lastRequestId += 1;
        return lastRequestId;
    }

    @Override
    public void tickPrice(int tickerId, int field, double price, int canAutoExecute) {
    }

    @Override
    public void tickSize(int tickerId, int field, int size) {
    }

    @Override
    public void tickOptionComputation(int tickerId, int field, double impliedVol, double delta, double optPrice, double pvDividend, double gamma, double vega, double theta, double undPrice) {
    }

    @Override
    public void tickGeneric(int tickerId, int tickType, double value) {
    }

    @Override
    public void tickString(int tickerId, int tickType, String value) {
    }

    @Override
    public void tickEFP(int tickerId, int tickType, double basisPoints, String formattedBasisPoints, double impliedFuture, int holdDays, String futureLastTradeDate, double dividendImpact, double dividendsToLastTradeDate) {
    }

    @Override
    public void orderStatus(int orderId, String status, int filled, int remaining, double avgFillPrice, int permId, int parentId, double lastFillPrice, int clientId, String whyHeld) {
    }

    @Override
    public void openOrder(int orderId, Contract contract, Order order, OrderState orderState) {

    }

    @Override
    public void openOrderEnd() {

    }

    @Override
    public void updateAccountValue(String key, String value, String currency, String accountName) {

    }

    @Override
    public void updatePortfolio(Contract contract, int position, double marketPrice, double marketValue, double averageCost, double unrealizedPNL, double realizedPNL, String accountName) {

    }

    @Override
    public void updateAccountTime(String timeStamp) {
        log.info(format("updateAccountTime: %s", timeStamp));
    }

    @Override
    public void accountDownloadEnd(String accountName) {
    }

    @Override
    public void nextValidId(int orderId) {
    }

    @Override
    public void contractDetails(int reqId, ContractDetails contractDetails) {
    }

    @Override
    public void bondContractDetails(int reqId, ContractDetails contractDetails) {

    }

    @Override
    public void contractDetailsEnd(int reqId) {

    }

    @Override
    public void execDetails(int reqId, Contract contract, Execution execution) {

    }

    @Override
    public void execDetailsEnd(int reqId) {

    }

    @Override
    public void updateMktDepth(int tickerId, int position, int operation, int side, double price, int size) {

    }

    @Override
    public void updateMktDepthL2(int tickerId, int position, String marketMaker, int operation, int side, double price, int size) {

    }

    @Override
    public void updateNewsBulletin(int msgId, int msgType, String message, String origExchange) {
        log.info(format("updateNewsBulletin: %s", message));
    }

    @Override
    public void managedAccounts(String accountsList) {

    }

    @Override
    public void receiveFA(int faDataType, String xml) {

    }

    @Override
    public void historicalData(int reqId, String date, double open, double high, double low, double close, int volume, int count, double WAP, boolean hasGaps) {

    }

    @Override
    public void scannerParameters(String xml) {

    }

    @Override
    public void scannerData(int reqId, int rank, ContractDetails contractDetails, String distance, String benchmark, String projection, String legsStr) {

    }

    @Override
    public void scannerDataEnd(int reqId) {

    }

    @Override
    public void realtimeBar(int reqId, long time, double open, double high, double low, double close, long volume, double wap, int count) {

    }

    @Override
    public void currentTime(long time) {
        log.info(format("currentTime: %s", time));
    }

    @Override
    public void fundamentalData(int reqId, String data) {

    }

    @Override
    public void deltaNeutralValidation(int reqId, DeltaNeutralContract underComp) {

    }

    @Override
    public void tickSnapshotEnd(int reqId) {

    }

    @Override
    public void marketDataType(int reqId, int marketDataType) {

    }

    @Override
    public void commissionReport(CommissionReport commissionReport) {

    }

    @Override
    public void position(String account, Contract contract, int pos, double avgCost) {

    }

    @Override
    public void positionEnd() {

    }

    @Override
    public void accountSummary(int reqId, String account, String tag, String value, String currency) {

    }

    @Override
    public void accountSummaryEnd(int reqId) {

    }

    @Override
    public void verifyMessageAPI(String apiData) {

    }

    @Override
    public void verifyCompleted(boolean isSuccessful, String errorText) {

    }

    @Override
    public void verifyAndAuthMessageAPI(String apiData, String xyzChallange) {

    }

    @Override
    public void verifyAndAuthCompleted(boolean isSuccessful, String errorText) {

    }

    @Override
    public void displayGroupList(int reqId, String groups) {

    }

    @Override
    public void displayGroupUpdated(int reqId, String contractInfo) {

    }

    @Override
    public void error(Exception e) {
        log.error("IBrokers callback wrapper error", e);
    }

    @Override
    public void error(String str) {
        log.error(str);
    }

    @Override
    public void error(int id, int errorCode, String errorMsg) {
        log.error(format("%s: error %s %s", id, errorCode, errorMsg));
        getErrors().add(new ImmutablePair<>(errorCode, errorMsg));
    }

    public ConcurrentLinkedQueue<Pair<Integer, String>> getErrors() {
        return errors;
    }

    @Override
    public void connectionClosed() {
        log.info(format("connectionClosed"));
    }

    @Override
    public void connectAck() {
        log.info("IBroker connect ACK");
        clientSocket.startAPI();
    }

}
