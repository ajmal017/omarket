package org.omarket.trading.ibrokers;

import com.ib.client.*;

import com.ib.contracts.StkContract;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Thread.sleep;

class MarketDataEWrapper implements EWrapper {
    private static Logger logger = LoggerFactory.getLogger(MarketDataEWrapper.class);
    private EClient m_client;

    public MarketDataEWrapper() {
    }

    public void setClient(EClient m_client){
        this.m_client = m_client;
    }

    @Override
    public void tickPrice(int tickerId, int field, double price, int canAutoExecute) {
        logger.info("received tick price): {}, {}", field, price);
    }

    @Override
    public void tickSize(int tickerId, int field, int size) {
        logger.info("received tick size): {}, {}", field, size);
    }

    @Override
    public void tickOptionComputation(int tickerId, int field, double impliedVol, double delta, double optPrice, double pvDividend, double gamma, double vega, double theta, double undPrice) {

    }

    @Override
    public void tickGeneric(int tickerId, int tickType, double value) {
        logger.info("received tick generic type {}: {}", tickType, value);
    }

    @Override
    public void tickString(int tickerId, int tickType, String value) {
        logger.info("received tick string type {}: {}", tickType, value);

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

    }

    @Override
    public void accountDownloadEnd(String accountName) {

    }

    @Override
    public void nextValidId(int orderId) {
        logger.info("received next valid Id: {}", orderId);
    }

    @Override
    public void contractDetails(int reqId, ContractDetails contractDetails) {
        try {
            logger.info("received contract details ({}): {}", reqId, contractDetails);
        } catch (Exception e) {
            logger.error(e.toString());
        }
    }

    @Override
    public void bondContractDetails(int reqId, ContractDetails contractDetails) {

    }

    @Override
    public void contractDetailsEnd(int reqId) {
        logger.info("received contract details end for request: {}", reqId);
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
        logger.info("current time: {}", time);
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
        logger.error("error: {}", e);
    }

    @Override
    public void error(String str) {
        logger.error(str);
    }

    @Override
    public void error(int id, int errorCode, String errorMsg) {
        logger.error(errorMsg);
    }

    @Override
    public void connectionClosed() {

    }

    @Override
    public void connectAck() {
        m_client.startAPI();
    }
}

/**
 * Created by Christophe on 26/09/2016.
 */
public class MarketData {
    private static Logger logger = LoggerFactory.getLogger(MarketDataEWrapper.class);

    public static void main(String[] args) throws InterruptedException {
        logger.info("starting market data");
        EReaderSignal readerSignal = new EJavaSignal();
        MarketDataEWrapper ewrapper = new MarketDataEWrapper();
        EClientSocket clientSocket = new EClientSocket(ewrapper, readerSignal);
        ewrapper.setClient(clientSocket);
        clientSocket.eConnect("127.0.0.1", 7497, 1);

        final EReader reader = new EReader(clientSocket, readerSignal);
        reader.start();

        new Thread() {
            public void run() {
                while (clientSocket.isConnected()) {
                    readerSignal.waitForSignal();
                    try {
                        logger.info("wait for signal");
                        reader.processMsgs();
                    } catch (Exception e) {
                        logger.error("Exception: {}", e);
                    }
                }
            }
        }.start();

        int counter = 3;
        do {
            clientSocket.reqCurrentTime();
            sleep(1000);
        } while (--counter > 0);

        Contract contract = new StkContract("IBM");

        //clientSocket.reqContractDetails(210, ContractSamples.OptionForQuery());
        //clientSocket.reqMktData(1004, ContractSamples.USStock(), "233,236,258", false, null);
        clientSocket.reqContractDetails(1, contract);


    }
}
