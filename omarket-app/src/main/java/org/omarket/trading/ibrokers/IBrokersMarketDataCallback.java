package org.omarket.trading.ibrokers;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.ib.client.Contract;
import com.ib.client.ContractDetails;
import io.vertx.rxjava.core.eventbus.EventBus;
import io.vertx.rxjava.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.omarket.trading.Security;
import org.omarket.trading.quote.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import org.omarket.trading.ContractDBService;
import org.springframework.stereotype.Component;

import static org.omarket.trading.MarketData.createChannelQuote;
import static org.omarket.trading.verticles.MarketDataVerticle.createSuccessReply;
import static org.omarket.trading.verticles.MarketDataVerticle.getErrorChannel;
import static org.omarket.trading.verticles.MarketDataVerticle.getErrorChannelGeneric;


/**
 * Created by Christophe on 03/11/2016.
 */
@Component
public class IBrokersMarketDataCallback extends AbstractIBrokersCallback {

    private final QuoteFactory quoteFactory;

    private Path contractDBPath;
    private ContractDBService contractDBService;

    private final static int PRICE_BID = 1;
    private final static int PRICE_ASK = 2;
    private final static int SIZE_BID = 0;
    private final static int SIZE_ASK = 3;
    private static Logger logger = LoggerFactory.getLogger(IBrokersMarketDataCallback.class);
    private Path storageDirPath;
    private final SimpleDateFormat formatYearMonthDay;
    private final SimpleDateFormat formatHour;
    private Integer lastRequestId = null;
    private Set<Integer> updateContractDB = new TreeSet<>();
    private Map<Integer, Message<JsonObject>> callbackMessages = new HashMap<>();
    private Map<Integer, Pair<MutableQuote, Contract>> orderBooks = new HashMap<>();
    private Map<Integer, Path> subscribed = new HashMap<>();
    private EventBus eventBus;
    private Map<Integer, String> eodReplies = new HashMap<>();

    @Autowired
    public IBrokersMarketDataCallback(ContractDBService contractDBService, QuoteFactory quoteFactory) {
        formatYearMonthDay = new SimpleDateFormat("yyyyMMdd");
        formatHour = new SimpleDateFormat("HH");
        formatYearMonthDay.setTimeZone(TimeZone.getTimeZone("UTC"));
        formatHour.setTimeZone(TimeZone.getTimeZone("UTC"));
        this.contractDBService = contractDBService;
        this.quoteFactory = quoteFactory;
    }

    private static Path prepareTickPath(Path storageDirPath, Security contractDetails) throws IOException {
        String code = contractDetails.getCode();
        Path productStorage = storageDirPath.resolve(createChannelQuote(code));
        logger.info("preparing storage for contract: " + productStorage);
        Files.createDirectories(productStorage);
        return productStorage;
    }

    synchronized private Integer newRequestId() {
        if (lastRequestId == null) {
            lastRequestId = 0;
        }
        lastRequestId += 1;
        return lastRequestId;
    }

    public String requestContract(Contract contract, Message<JsonObject> message) {
        Integer newRequestId = newRequestId();
        callbackMessages.put(newRequestId, message);
        getClient().reqContractDetails(newRequestId, contract);
        return getErrorChannel(newRequestId);
    }

    public String requestContract(Contract contract) {
        Integer newRequestId = newRequestId();
        this.updateContractDB.add(newRequestId);
        getClient().reqContractDetails(newRequestId, contract);
        return getErrorChannel(newRequestId);
    }

    public String eod(Security contractDetails, String replyAddress){
        int requestId = newRequestId();
        ZonedDateTime endDate = ZonedDateTime.of(LocalDateTime.now(), ZoneId.from(ZoneOffset.UTC));
        DateTimeFormatter ibrokersFormat = DateTimeFormatter.ofPattern("yyyyMMdd hh:mm:ss");
        String endDateString = endDate.format(ibrokersFormat);
        String duration = "3 M";
        String bar = "1 day";
        String what = "TRADES";
        int rth = 1;
        int useLongDate = 2;
        eodReplies.put(requestId, replyAddress);
        getClient().reqHistoricalData(requestId, contractDetails.toContractDetails().contract(), endDateString, duration , bar, what, rth, useLongDate, null);
        return getErrorChannel(requestId);
    }

    public String subscribe(Security security) throws IOException {
        int requestId = newRequestId();
        Contract contract = security.toContractDetails().contract();
        Integer ibCode = contract.conid();
        if (subscribed.containsKey(ibCode)) {
            logger.info("already subscribed: " + ibCode);
            return null;
        }
        Files.createDirectories(storageDirPath);
        Path productStorage = prepareTickPath(storageDirPath, security);
        contractDBService.saveContract(contractDBPath, security);
        subscribed.put(ibCode, productStorage);
        MutableQuote quote = quoteFactory.createMutable(security.getMinTick(), security.getCode());
        orderBooks.put(requestId, new ImmutablePair<>(quote, contract));
        logger.info("requesting market data for " + security);
        getClient().reqMktData(requestId, contract, "", false, null);
        return getErrorChannel(requestId);
    }

    @Override
    public void contractDetails(int requestId, ContractDetails contractDetails) {
        Gson gson = new GsonBuilder().create();
        JsonObject product = new JsonObject(gson.toJson(contractDetails));
        if(callbackMessages.containsKey(requestId)) {
            Message<JsonObject> message = callbackMessages.get(requestId);
            message.reply(createSuccessReply(product));
        }
        if(updateContractDB.contains(requestId)) {
            Security security = Security.fromContractDetails(contractDetails);
            try {
                contractDBService.saveContract(Paths.get("data", "contracts"), security);
                updateContractDB.remove(requestId);
            } catch (IOException e) {
                logger.error("failed to update contract db", e);
            }
        }
    }

    @Override
    public void contractDetailsEnd(int currentRequestId) {
        logger.debug("received contract details end for request: {}", currentRequestId);
    }

    @Override
    public void currentTime(long time) {
        logger.info("requested current time: {}", time);
    }

    @Override
    public void tickPrice(int tickerId, int field, double price, int canAutoExecute) {
        if (field != PRICE_BID && field != PRICE_ASK) {
            return;
        }
        Pair<MutableQuote, Contract> orderBookContract = orderBooks.get(tickerId);
        MutableQuote orderBook = orderBookContract.getLeft();
        boolean modified;
        if (field == PRICE_BID) {
            modified = orderBook.updateBestBidPrice(price);
        } else {
            modified = orderBook.updateBestAskPrice(price);
        }
        if (!orderBook.isValid() || !modified) {
            return;
        }
        Contract contract = orderBookContract.getRight();
        processOrderBook(contract, orderBook);
    }

    @Override
    public void tickSize(int tickerId, int field, int size) {
        if (field != SIZE_BID && field != SIZE_ASK) {
            return;
        }
        Pair<MutableQuote, Contract> orderBookContract = orderBooks.get(tickerId);
        MutableQuote orderBook = orderBookContract.getLeft();
        boolean modified;
        if (field == SIZE_BID) {
            modified = orderBook.updateBestBidSize(size);
        } else {
            modified = orderBook.updateBestAskSize(size);
        }
        if (!orderBook.isValid() || !modified) {
            return;
        }
        Contract contract = orderBookContract.getRight();
        processOrderBook(contract, orderBook);
    }

    private void processOrderBook(Contract contract, Quote orderBook) {
        String channel = createChannelQuote(String.valueOf(contract.conid()));
        try {
            Path rootDirectory = subscribed.get(contract.conid());
            Date now = new Date();
            Path currentDirectory = rootDirectory.resolve(formatYearMonthDay.format(now));
            Files.createDirectories(currentDirectory);
            Path tickFilePath = currentDirectory.resolve(formatHour.format(now));
            String content = QuoteConverter.toPriceVolumeString(orderBook);
            if (!Files.exists(tickFilePath)) {
                Files.createFile(tickFilePath);
            }
            BufferedWriter writer =
                    Files.newBufferedWriter(tickFilePath, StandardCharsets.UTF_8, StandardOpenOption.APPEND);
            writer.write(content);
            writer.newLine();
            writer.close();
            this.eventBus.send(channel, QuoteConverter.toJSON(orderBook));
        } catch (IOException e) {
            logger.error("unable to record order book: message not sent", e);
        }
    }
    @Override
    public void historicalData(int reqId, String date, double open, double high, double low, double close, int volume,
                               int count, double WAP, boolean hasGaps) {
        String replyData = eodReplies.get(reqId);
        JsonObject bar = new JsonObject();
        bar.put("date", date);
        bar.put("open", open);
        bar.put("high", high);
        bar.put("low", low);
        bar.put("close", close);
        bar.put("volume", volume);
        bar.put("count", count);
        bar.put("WAP", WAP);
        bar.put("hasGaps", hasGaps);
        this.eventBus.send(replyData, bar);
    }

    @Override
    public void error(int requestId, int errorCode, String errorMsg) {
        Map<Integer, String> exceptions = new HashMap<>();
        exceptions.put(2104, "Market data farm connection is OK");
        exceptions.put(2106, "HMDS data farm connection is OK");
        if (!exceptions.containsKey(errorCode)) {
            if (requestId != -1) {
                logger.error("error code: " + errorCode + " - " + errorMsg + " (request id: " + requestId + ")");
                JsonObject errorJson = new JsonObject();
                errorJson.put("code", errorCode);
                errorJson.put("message", errorMsg);
                this.eventBus.send(getErrorChannel(requestId), errorJson);
            } else {
                if(errorCode == 2110){
                    // Connectivity between Trader Workstation and server is broken. It will be restored automatically.
                    logger.error("error code: " + errorCode + " - " + errorMsg);
                    JsonObject errorJson = new JsonObject();
                    errorJson.put("code", errorCode);
                    errorJson.put("message", errorMsg);
                    this.eventBus.send(getErrorChannelGeneric(), errorJson);
                } else if (errorCode == 1102){
                    // Connectivity between IB and Trader Workstation has been restored - data maintained.
                    logger.error("error code: " + errorCode + " - " + errorMsg);
                    JsonObject errorJson = new JsonObject();
                    errorJson.put("code", errorCode);
                    errorJson.put("message", errorMsg);
                    this.eventBus.send(getErrorChannelGeneric(), errorJson);
                } else {
                    // Connectivity between IB and Trader Workstation has been restored - data maintained.
                    logger.error("error code: " + errorCode + " - " + errorMsg);
                    JsonObject errorJson = new JsonObject();
                    errorJson.put("code", errorCode);
                    errorJson.put("message", errorMsg);
                    this.eventBus.send(getErrorChannelGeneric(), errorJson);
                }
            }
        }
    }

    @Override
    public void error(Exception e) {
        logger.error("IBrokers callback wrapper error", e);
    }

    @Override
    public void error(String str) {
        logger.error(str);
    }

    public void setEventBus(EventBus eventBus) {
        this.eventBus = eventBus;
    }

    public void setStorageDirPath(Path storageDirPath) {
        this.storageDirPath = storageDirPath;
    }

    public void setContractDBPath(Path contractDBPath) {
        this.contractDBPath = contractDBPath;
    }

    public void setContractDBService(ContractDBService contractDBService) {
        this.contractDBService = contractDBService;
    }
}
