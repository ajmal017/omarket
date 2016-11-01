package org.omarket.trading.ibrokers;

import com.ib.client.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static java.lang.Thread.sleep;

abstract class AbstractEWrapper implements EWrapper {
    private static Logger logger = LoggerFactory.getLogger(AbstractEWrapper.class);
    private EClient m_client;

    public void setClient(EClient m_client){
        this.m_client = m_client;
    }

    public EClient getClient(){
        return this.m_client;
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
        logger.error("IBrokers callback wrapper error", e);
    }

    @Override
    public void error(String str) {
        logger.error(str);
    }

    @Override
    public void error(int id, int errorCode, String errorMsg) {

    }

    @Override
    public void connectionClosed() {

    }

    @Override
    public void connectAck() {
        m_client.startAPI();
    }
}

class ContractDetailsEWrapper extends AbstractEWrapper {
    private static Logger logger = LoggerFactory.getLogger(ContractDetailsEWrapper.class);
    private Map<Integer, Contract> pipeline = new HashMap<>();
    private Integer lastRequestId = null;
    private Boolean isCompleted = false;

    ContractDetailsEWrapper() {
    }

    private Integer newRequestId(){
        if (lastRequestId == null){
            lastRequestId = 0;
        }
        lastRequestId += 1;
        return lastRequestId;
    }

    void addRequest(Contract contract){
        Integer newRequestId = newRequestId();
        pipeline.put(newRequestId, contract);
    }

    void processRequests() {
        for (Integer requestId: pipeline.keySet()){
            Contract contract = pipeline.get(requestId);
            getClient().reqContractDetails(requestId, contract);
        }
    }

    @Override
    public void connectionClosed() {

    }

    @Override
    public void contractDetails(int requestId, ContractDetails contractDetails) {
        try {
            logger.info("received contract details ({}): {}", requestId, contractDetails);
        } catch (Exception e) {
            logger.error(e.toString());
        }
    }

    @Override
    public void contractDetailsEnd(int requestId) {
        pipeline.remove(requestId);
        if (pipeline.isEmpty()){
            logger.info("No more request to be processed");
            setCompleted(true);
        }
        logger.info("received contract details end for request: {}", requestId);
    }

    @Override
    public void currentTime(long time) {
        logger.info("requested current time: {}", time);
    }

    Boolean getCompleted() {
        return isCompleted;
    }

    private void setCompleted(Boolean completed) {
        isCompleted = completed;
    }
}

/**
 * Created by Christophe on 26/09/2016.
 */
public class MarketData {
    private static Logger logger = LoggerFactory.getLogger(ContractDetailsEWrapper.class);

    public static void main(String[] args) throws InterruptedException {
        logger.info("starting market data");
        EReaderSignal readerSignal = new EJavaSignal();
        ContractDetailsEWrapper ewrapper = new ContractDetailsEWrapper();
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
                while (!ewrapper.getCompleted() && clientSocket.isConnected()) {
                    readerSignal.waitForSignal();
                    try {
                        logger.info("IBrokers thread waiting for signal");
                        reader.processMsgs();
                    } catch (Exception e) {
                        logger.error("Exception: {}", e);
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
