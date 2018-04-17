package org.omarket.trading.verticles;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParser;
import com.ib.client.Contract;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.eventbus.Message;
import io.vertx.rxjava.core.eventbus.MessageConsumer;
import lombok.extern.slf4j.Slf4j;
import org.omarket.trading.ContractDBService;
import org.omarket.trading.Security;
import org.omarket.ibroker.IBrokerConnectionFailure;
import org.omarket.trading.VertxIBrokerClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import rx.Observable;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Christophe on 01/11/2016.
 */
@Slf4j
@Component
public class MarketDataVerticle extends AbstractVerticle {

    public static final JsonObject EMPTY = new JsonObject();
    private final static Map<String, Security> subscribedProducts = new HashMap<>();
    private final ContractDBService contractDBService;

    private final VertxIBrokerClient ibrokersClient;
    @Value("${ibrokers.host}")
    private String ibrokersHost;
    @Value("${ibrokers.port}")
    private String ibrokersPort;
    @Value("${org.omarket.client_id.record_prices}")
    private String ibrokersClientId;
    @Value("${ibrokers.ticks.storagePath}")
    private String storageDir;
    @Value("${oot.contracts.dbPath}")
    private String contractDB;
    @Value("${address.subscribe_tick}")
    private String ADDRESS_SUBSCRIBE_TICK;
    @Value("${address.eod_request}")
    private String ADDRESS_EOD_REQUEST;
    @Value("${address.eod_data_prefix}")
    private String ADDRESS_EOD_DATA_PREFIX;
    @Value("${address.unsubscribe_tick}")
    private String ADDRESS_UNSUBSCRIBE_TICK;
    @Value("${address.unsubscribe_all}")
    private String ADDRESS_UNSUBSCRIBE_ALL;
    @Value("${address.contract_retrieve}")
    private String ADDRESS_CONTRACT_RETRIEVE;
    @Value("${address.contract_download}")
    private String ADDRESS_CONTRACT_DOWNLOAD;
    @Value("${address.order_book_level_one}")
    private String ADDRESS_ORDER_BOOK_LEVEL_ONE;
    @Value("${address.admin_command}")
    private String ADDRESS_ADMIN_COMMAND;
    @Value("${address.error_message_prefix}")
    private String ADDRESS_ERROR_MESSAGE_PREFIX;

    @Autowired
    public MarketDataVerticle(ContractDBService contractDBService, VertxIBrokerClient ibrokersClient) {
        this.contractDBService = contractDBService;
        this.ibrokersClient = ibrokersClient;
    }

    public void preStart() throws IBrokerConnectionFailure {
        Path storageDirPath = Paths.get(storageDir).toAbsolutePath();
        Path contractDBPath = Paths.get(contractDB).toAbsolutePath();
        log.info("ticks data storage set to '" + storageDirPath + "'");
        ibrokersClient.setContractDBService(contractDBService);
        ibrokersClient.setStorageDirPath(storageDirPath);
        ibrokersClient.setContractDBPath(contractDBPath);
        ibrokersClient.connect(Integer.valueOf(ibrokersClientId), ibrokersHost, Integer.valueOf(ibrokersPort));
    }

    public void start(Future<Void> startFuture) throws Exception {
        log.info("starting market data");
        ibrokersClient.setEventBus(vertx.eventBus());
        vertx.executeBlocking(future -> {
            try {
                ibrokersClient.startMessageProcessing();
                setupContractDownload();
                setupHistoricalEOD();
                setupSubscribeTick();
                setupUnsubscribeTick();
                future.complete();
            } catch (IOException ioe) {
                log.error("error during message processing", ioe);
                future.fail(ioe);
            }
        }, result -> {
            if (result.succeeded()) {
                log.info("started market data verticle");
                startFuture.complete();
            } else {
                log.info("failed to start market data verticle");
                startFuture.fail("failed to start market data verticle");
            }
        });
    }

    private static JsonObject createErrorReply(JsonObject errorMessage, JsonObject content) {
        JsonObject result = new JsonObject();
        result.put("error", errorMessage);
        result.put("content", content);
        return result;
    }

    public static JsonObject createSuccessReply(JsonObject content) {
        JsonObject result = new JsonObject();
        result.put("error", EMPTY);
        result.put("content", content);
        return result;
    }

    private void setupSubscribeTick() {
        Observable<Message<JsonObject>> consumer =
                vertx.eventBus().<JsonObject>consumer(ADDRESS_SUBSCRIBE_TICK).toObservable();
        consumer.subscribe(message -> {
            io.vertx.core.json.JsonObject vertxBody = message.body();
            String stringSecurity = vertxBody.encode();
            JsonParser parser = new JsonParser();
            com.google.gson.JsonObject googleJson = parser.parse(stringSecurity).getAsJsonObject();
            final Security security = Security.fromJson(googleJson);
            log.info("received tick subscription request for: " + security);
            String productCode = security.getCode();
            if (!subscribedProducts.containsKey(productCode)) {
                vertx.executeBlocking(future -> {
                    try {
                        log.info("subscribing: " + productCode);
                        BigDecimal minTick = security.getMinTick();
                        subscribedProducts.put(productCode, security);
                        String errorChannel = ibrokersClient.subscribe(security);
                        if (errorChannel != null) {
                            Observable<JsonObject> errorStream =
                                    vertx.eventBus().<JsonObject>consumer(errorChannel).bodyStream().toObservable();
                            errorStream.subscribe(errorMessage -> {
                                log.error("failed to subscribe for contract '" + productCode + "': " + errorMessage);
                            });
                        }
                        future.complete(productCode);
                    } catch (Exception e) {
                        log.error("failed to subscribe product: '" + productCode + "'", e);
                        future.fail(e);
                    }
                }, result -> {
                    log.info("raw result: " + result.result());
                    String status = "failed";
                    if (result.succeeded()) {
                        log.info("subscription succeeded for " + productCode);
                        status = "registered";
                    } else {
                        log.info("subscription failed for " + productCode);
                    }
                    final JsonObject reply = new JsonObject().put("status", status);
                    message.reply(reply);
                });
            } else {
                log.info("already registered: " + productCode);
                final JsonObject reply = new JsonObject().put("status", "already_registered");
                message.reply(reply);
            }
        });
        log.info("quotes subscription service deployed");
    }

    private void setupUnsubscribeTick() {
        Observable<Message<JsonObject>> consumer =
                vertx.eventBus().<JsonObject>consumer(ADDRESS_UNSUBSCRIBE_TICK).toObservable();
        consumer.subscribe(message -> {
            final JsonObject contract = message.body();
            log.info("received unsubscription request for: " + contract);
            String status = "failed";
            String productCode = contract.getString("conId");
            if (subscribedProducts.containsKey(productCode)) {
                // un-subscription takes place here
                subscribedProducts.remove(productCode);
                status = "unsubscribed";
            } else {
                status = "missing";
            }
            final JsonObject reply = new JsonObject().put("status", status);
            message.reply(reply);
        });
    }

    private void setupContractDownload() {
        final MessageConsumer<JsonObject> consumer = vertx.eventBus().consumer(ADDRESS_CONTRACT_DOWNLOAD);
        Observable<Message<JsonObject>> contractStream = consumer.toObservable();
        contractStream.subscribe(message -> {
            final JsonObject body = message.body();
            log.info("requesting contract: " + body);
            final int productCode = Integer.parseInt(body.getString("conId"));
            Contract contract = new Contract();
            contract.conid(productCode);
            String errorChannel = ibrokersClient.requestContract(contract);
            Observable<JsonObject> errorStream =
                    vertx.eventBus().<JsonObject>consumer(errorChannel).bodyStream().toObservable();
            errorStream.subscribe(errorMessage -> {
                log.error("failed to request contract '" + productCode + "': " + errorMessage);
                Gson gson = new GsonBuilder().create();
                JsonObject product = new JsonObject(gson.toJson(contract));
                message.reply(createErrorReply(errorMessage, product));
            });
            message.reply(createSuccessReply(body));
        });
    }

    private void setupHistoricalEOD() {
        final MessageConsumer<JsonObject> consumer = vertx.eventBus().consumer(ADDRESS_EOD_REQUEST);
        Observable<Message<JsonObject>> histRequestStream = consumer.toObservable();
        histRequestStream.subscribe(request -> {
            io.vertx.core.json.JsonObject vertxBody = request.body();
            String stringSecurity = vertxBody.encode();
            JsonParser parser = new JsonParser();
            com.google.gson.JsonObject googleJson = parser.parse(stringSecurity).getAsJsonObject();
            final Security contractDetails = Security.fromJson(googleJson);
            String productCode = contractDetails.getCode();
            log.info("received historical eod request for: " + productCode);
            String replyAddress = ADDRESS_EOD_DATA_PREFIX + "." + productCode;
            String errorChannel = ibrokersClient.eod(contractDetails, replyAddress);
            if (errorChannel != null) {
                MessageConsumer<JsonObject> errorConsumer = vertx.eventBus().consumer(errorChannel);
                Observable<JsonObject> errorStream = errorConsumer.bodyStream().toObservable();
                errorStream.subscribe(errorMessage -> {
                    log.error("failed to subscribe for contract '" + productCode + "': " + errorMessage);
                });
            }
            final JsonArray bars = new JsonArray();
            vertx.eventBus().<JsonObject>consumer(replyAddress, handler -> {
                JsonObject bar = handler.body();
                boolean completed = false;
                String lastMarker = "finished-";
                if (bar.getString("date").startsWith(lastMarker)) {
                    bar.put("date", bar.getString("date").substring(lastMarker.length()));
                    completed = true;
                }
                bars.add(bar);
                if (completed) {
                    request.reply(bars);
                }
            });
        });
    }

}
