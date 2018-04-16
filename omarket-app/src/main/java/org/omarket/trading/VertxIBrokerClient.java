package org.omarket.trading;

import com.ib.client.Contract;
import com.ib.client.ContractDetails;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.core.eventbus.EventBus;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.omarket.trading.ContractDBService;
import org.omarket.trading.Security;
import org.omarket.trading.ibroker.AbstractIBrokerClient;
import org.omarket.trading.ibroker.MarketData;
import org.omarket.trading.quote.MutableQuote;
import org.omarket.trading.quote.Quote;
import org.omarket.trading.quote.QuoteConverter;
import org.omarket.trading.quote.QuoteFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

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
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeSet;

import static java.lang.String.format;
import static java.lang.Thread.sleep;


/**
 * Created by Christophe on 03/11/2016.
 */
@Slf4j
@Component
public class VertxIBrokerClient extends AbstractIBrokerClient {

    public static final int IB_MAX_SIMULTANEOUS_CONTRACT_DETAILS_REQUESTS = 10;
    public static final int PERIOD_CONTRACT_DETAILS_REQUEST_RETRY_MS = 1000;
    @Value("${address.error_message_prefix}")
    private String ADDRESS_ERROR_MESSAGE_PREFIX;

    private final static int PRICE_BID = 1;
    private final static int PRICE_ASK = 2;
    private final static int SIZE_BID = 0;
    private final static int SIZE_ASK = 3;
    private final QuoteFactory quoteFactory;
    private final SimpleDateFormat formatYearMonthDay;
    private final SimpleDateFormat formatHour;
    private Path contractDBPath;
    private ContractDBService contractDBService;
    private MarketData marketData;
    private Path storageDirPath;
    private EventBus eventBus;
    private Set<Integer> requestsContractDetails = Collections.synchronizedSet(new TreeSet<>());
    private Map<Integer, Pair<MutableQuote, Contract>> orderBooks = new HashMap<>();
    private Map<Integer, Path> subscribed = new HashMap<>();
    private Map<Integer, String> eodReplies = new HashMap<>();

    @Autowired
    public VertxIBrokerClient(ContractDBService contractDBService, QuoteFactory quoteFactory, MarketData marketData) {
        formatYearMonthDay = new SimpleDateFormat("yyyyMMdd");
        formatHour = new SimpleDateFormat("HH");
        formatYearMonthDay.setTimeZone(TimeZone.getTimeZone("UTC"));
        formatHour.setTimeZone(TimeZone.getTimeZone("UTC"));
        this.contractDBService = contractDBService;
        this.quoteFactory = quoteFactory;
        this.marketData = marketData;
    }

    private Path prepareTickPath(Path storageDirPath, Security contractDetails) throws IOException {
        String code = contractDetails.getCode();
        Path productStorage = storageDirPath.resolve(marketData.createChannelQuote(code));
        log.info("preparing storage for contract: " + productStorage);
        Files.createDirectories(productStorage);
        return productStorage;
    }

    public String getErrorChannel(Integer requestId) {
        return ADDRESS_ERROR_MESSAGE_PREFIX + "." + requestId;
    }

    /**
     *
     * @param contract
     * @return
     */
    public String requestContract(Contract contract) {
        Integer newRequestId = newRequestId();
        while(this.requestsContractDetails.size() >= IB_MAX_SIMULTANEOUS_CONTRACT_DETAILS_REQUESTS){
            try {
                sleep(PERIOD_CONTRACT_DETAILS_REQUEST_RETRY_MS);
            } catch (InterruptedException e) {
                log.error("interrupted request for contract details", e);
            }
        }
        this.requestsContractDetails.add(newRequestId);
        log.info(format("request contract details: id %s", newRequestId));
        getClientSocket().reqContractDetails(newRequestId, contract);
        return getErrorChannel(newRequestId);
    }

    public String eod(Security contractDetails, String replyAddress) {
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
        getClientSocket().reqHistoricalData(requestId, contractDetails.toContractDetails().contract(), endDateString, duration, bar, what, rth, useLongDate, null);
        return getErrorChannel(requestId);
    }

    public String subscribe(Security security) throws IOException {
        int requestId = newRequestId();
        Contract contract = security.toContractDetails().contract();
        Integer ibCode = contract.conid();
        if (subscribed.containsKey(ibCode)) {
            log.info("already subscribed: " + ibCode);
            return null;
        }
        Files.createDirectories(storageDirPath);
        Path productStorage = prepareTickPath(storageDirPath, security);
        contractDBService.saveContract(contractDBPath, security);
        subscribed.put(ibCode, productStorage);
        MutableQuote quote = quoteFactory.createMutable(security.getMinTick(), security.getCode());
        orderBooks.put(requestId, new ImmutablePair<>(quote, contract));
        log.info("requesting market data for " + security);
        getClientSocket().reqMktData(requestId, contract, "", false, null);
        return getErrorChannel(requestId);
    }

    @Override
    public void contractDetails(int requestId, ContractDetails contractDetails) {
        if (requestsContractDetails.contains(requestId)) {
            Security security = Security.fromContractDetails(contractDetails);
            try {
                contractDBService.saveContract(Paths.get("data", "contracts"), security);
                requestsContractDetails.remove(requestId);
            } catch (IOException e) {
                log.error("failed to update contract db", e);
            }
        }
    }

    @Override
    public void contractDetailsEnd(int currentRequestId) {
        log.debug("received contract details end for request: {}", currentRequestId);
    }

    @Override
    public void currentTime(long time) {
        log.info("requested current time: {}", time);
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
        String channel = marketData.createChannelQuote(String.valueOf(contract.conid()));
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
            log.debug("sending order book {}", orderBook);
            this.eventBus.send(channel, QuoteConverter.toJSON(orderBook));
        } catch (IOException e) {
            log.error("unable to record order book: message not sent", e);
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
        super.error(requestId, errorCode, errorMsg);
        Map<Integer, String> ignores = new HashMap<>();
        ignores.put(2104, "Market data farm connection is OK");
        ignores.put(2106, "HMDS data farm connection is OK");
        if (!ignores.containsKey(errorCode)) {
            if (requestId != -1) {
                log.error("error code: " + errorCode + " - " + errorMsg + " (request id: " + requestId + ")");
                JsonObject errorJson = new JsonObject();
                errorJson.put("code", errorCode);
                errorJson.put("message", errorMsg);
                this.eventBus.send(getErrorChannel(requestId), errorJson);
            } else {
                log.error("error code: " + errorCode + " - " + errorMsg);
                if (errorCode == 2110) {
                    // Connectivity between Trader Workstation and server is broken. It will be restored automatically.
                    JsonObject errorJson = new JsonObject();
                    errorJson.put("code", errorCode);
                    errorJson.put("message", errorMsg);
                    this.eventBus.send(getErrorChannelGeneric(), errorJson);
                } else if (errorCode == 1102) {
                    // Connectivity between IB and Trader Workstation has been restored - data maintained.
                    JsonObject errorJson = new JsonObject();
                    errorJson.put("code", errorCode);
                    errorJson.put("message", errorMsg);
                    this.eventBus.send(getErrorChannelGeneric(), errorJson);
                } else {
                    // Connectivity between IB and Trader Workstation has been restored - data maintained.
                    JsonObject errorJson = new JsonObject();
                    errorJson.put("code", errorCode);
                    errorJson.put("message", errorMsg);
                    this.eventBus.send(getErrorChannelGeneric(), errorJson);
                }
            }
        }
    }

    public String getErrorChannelGeneric() {
        return ADDRESS_ERROR_MESSAGE_PREFIX + ".*";
    }

    @Override
    public void error(Exception e) {
        log.error("IBrokers callback wrapper error", e);
    }

    @Override
    public void error(String str) {
        log.error(str);
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
