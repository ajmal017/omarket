package org.omarket.trading.verticles;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParser;
import com.ib.client.Contract;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.eventbus.Message;
import io.vertx.rxjava.core.eventbus.MessageConsumer;
import lombok.extern.slf4j.Slf4j;
import org.omarket.trading.ContractDBService;
import org.omarket.trading.Security;
import org.omarket.trading.ibroker.IBrokersConnectionFailure;
import org.omarket.trading.ibroker.VertxIBrokerClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import rx.Observable;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

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

    private static String getProductAsString(String ibCode) {
        Security product = subscribedProducts.get(ibCode);
        if (product == null) {
            log.error("product not found for : " + ibCode);
            return null;
        }
        return product.toString();
    }

    private static Set<String> getSubscribedProducts() {
        return subscribedProducts.keySet();
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

    private void setupSubscribeTick(Vertx vertx, VertxIBrokerClient ibrokersClient) {
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

    private void setupUnsubscribeTick(Vertx vertx) {
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

    private void setupContractRetrieve(Vertx vertx, VertxIBrokerClient ibrokersClient) {
        final MessageConsumer<JsonObject> consumer = vertx.eventBus().consumer(ADDRESS_CONTRACT_RETRIEVE);
        Observable<Message<JsonObject>> contractStream = consumer.toObservable();
        contractStream.subscribe(message -> {
            final JsonObject body = message.body();
            log.info("requesting contract: " + body);
            final int productCode = Integer.parseInt(body.getString("conId"));
            Contract contract = new Contract();
            contract.conid(productCode);
            String errorChannel = ibrokersClient.requestContract(contract, message);
            Observable<JsonObject> errorStream =
                    vertx.eventBus().<JsonObject>consumer(errorChannel).bodyStream().toObservable();
            errorStream.subscribe(errorMessage -> {
                log.error("failed to request contract '" + productCode + "': " + errorMessage);
                Gson gson = new GsonBuilder().create();
                JsonObject product = new JsonObject(gson.toJson(contract));
                message.reply(createErrorReply(errorMessage, product));
            });
        });
    }

    private void setupContractDownload(Vertx vertx, VertxIBrokerClient ibrokersClient) {
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

    private void setupAdminCommand(Vertx vertx) {
        Observable<Message<String>> consumer = vertx.eventBus().<String>consumer(ADDRESS_ADMIN_COMMAND).toObservable();
        consumer.subscribe(message -> {
            final String commandLine = message.body();
            String[] fields = commandLine.split("\\s+");
            String command = fields[0];
            String[] args = {};
            if (fields.length >= 2) {
                args = Arrays.copyOfRange(fields, 1, fields.length);
            }
            String result = "";
            switch (command) {
                case "subscribed":
                    result = String.join(", ", getSubscribedProducts());
                    break;
                case "details":
                    String ibCode = args[0];
                    result = getProductAsString(ibCode);
                    break;
                case "help":
                    result = "available commands: subscribed, details";
                    break;
            }
            message.reply(result);
        });
    }

    public void adminCommand(Vertx vertx, String commandLine) {
        vertx.eventBus().send(ADDRESS_ADMIN_COMMAND, commandLine, reply -> {
            if (reply.succeeded()) {
                String commandResult = (String) reply.result().body();
                log.debug(commandLine + " -> '" + commandResult + "'");
            } else {
                log.error("failed to run admin command '" + commandLine + "'");
            }
        });
    }

    private void setupHistoricalEOD(Vertx vertx, VertxIBrokerClient ibrokersClient) {
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

    public void start(Future<Void> startFuture) throws Exception {
        log.info("starting market data");
        Path storageDirPath = Paths.get(storageDir).toAbsolutePath();
        Path contractDBPath = Paths.get(contractDB).toAbsolutePath();
        log.info("ticks data storage set to '" + storageDirPath + "'");

        ibrokersClient.setContractDBService(contractDBService);
        ibrokersClient.setEventBus(vertx.eventBus());
        ibrokersClient.setStorageDirPath(storageDirPath);
        ibrokersClient.setContractDBPath(contractDBPath);
        vertx.executeBlocking(future -> {
            try {
                ibrokersClient.connect(Integer.valueOf(ibrokersClientId), ibrokersHost, Integer.valueOf(ibrokersPort));
                ibrokersClient.startMessageProcessing();
                setupContractRetrieve(vertx, ibrokersClient);
                setupContractDownload(vertx, ibrokersClient);
                setupHistoricalEOD(vertx, ibrokersClient);
                setupSubscribeTick(vertx, ibrokersClient);
                setupUnsubscribeTick(vertx);
                setupAdminCommand(vertx);
                future.complete();
            } catch (IBrokersConnectionFailure iBrokersConnectionFailure) {
                log.error("connection failed", iBrokersConnectionFailure);
                future.fail(iBrokersConnectionFailure);
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

}
