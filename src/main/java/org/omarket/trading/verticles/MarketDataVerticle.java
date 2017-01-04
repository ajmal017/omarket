package org.omarket.trading.verticles;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.ib.client.Contract;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.rx.java.ObservableFuture;
import io.vertx.rx.java.RxHelper;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.rxjava.core.eventbus.MessageConsumer;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.omarket.trading.MarketData;
import org.omarket.trading.ibrokers.IBrokersConnectionFailure;
import org.omarket.trading.ibrokers.IBrokersMarketDataCallback;
import rx.Observable;
import rx.functions.Action2;

import java.io.*;
import java.math.BigDecimal;
import java.math.MathContext;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.*;

/**
 * Created by Christophe on 01/11/2016.
 */
public class MarketDataVerticle extends AbstractVerticle {
    public final static String ADDRESS_SUBSCRIBE_TICK = "oot.marketData.subscribeTick";
    public final static String ADDRESS_EOD_REQUEST = "oot.marketData.subscribeDaily";
    public static final String ADDRESS_EOD_DATA_PREFIX = "oot.marketData.hist";
    public final static String ADDRESS_UNSUBSCRIBE_TICK = "oot.marketData.unsubscribeTick";
    public final static String ADDRESS_UNSUBSCRIBE_ALL = "oot.marketData.unsubscribeAll";
    public final static String ADDRESS_CONTRACT_RETRIEVE = "oot.marketData.contractRetrieve";
    public final static String ADDRESS_ORDER_BOOK_LEVEL_ONE = "oot.marketData.orderBookLevelOne";
    public final static String ADDRESS_ADMIN_COMMAND = "oot.marketData.adminCommand";
    public final static String ADDRESS_ERROR_MESSAGE_PREFIX = "oot.marketData.error";
    public static final JsonObject EMPTY = new JsonObject();
    private final static Logger logger = LoggerFactory.getLogger(MarketDataVerticle.class.getName());
    private final static Map<String, JsonObject> subscribedProducts = new HashMap<>();

    public static String getErrorChannel(Integer requestId) {
        return ADDRESS_ERROR_MESSAGE_PREFIX + "." + requestId;
    }

    public static String getErrorChannelGeneric() {
        return ADDRESS_ERROR_MESSAGE_PREFIX + ".*";
    }

    private static String getProductAsString(String ibCode) {
        JsonObject product = subscribedProducts.get(ibCode);
        if (product == null) {
            logger.error("product not found for : " + ibCode);
            return null;
        }
        return product.toString();
    }

    private static Set<String> getSubscribedProducts() {
        return subscribedProducts.keySet();
    }

    private static void setupSubscribeTick(Vertx vertx, IBrokersMarketDataCallback ibrokersClient) {
        Observable<Message<JsonObject>> consumer =
                vertx.eventBus().<JsonObject>consumer(ADDRESS_SUBSCRIBE_TICK).toObservable();
        consumer.subscribe(message -> {
            final JsonObject contractDetails = message.body();
            logger.info("received tick subscription request for: " + contractDetails);
            JsonObject contractJson = contractDetails.getJsonObject("m_contract");
            Integer productCode = contractJson.getInteger("m_conid");
            if (!subscribedProducts.containsKey(Integer.toString(productCode))) {
                vertx.executeBlocking(future -> {
                    try {
                        logger.info("subscribing: " + productCode.toString());
                        Double minTick = contractDetails.getDouble("m_minTick");
                        subscribedProducts.put(Integer.toString(productCode), contractDetails);
                        String errorChannel =
                                ibrokersClient.subscribe(contractDetails, new BigDecimal(minTick, MathContext
                                        .DECIMAL32).stripTrailingZeros());
                        if (errorChannel != null) {
                            Observable<JsonObject> errorStream =
                                    vertx.eventBus().<JsonObject>consumer(errorChannel).bodyStream().toObservable();
                            errorStream.subscribe(errorMessage -> {
                                logger.error("failed to subscribe for contract '" + productCode + "': " + errorMessage);
                            });
                        }
                        future.complete(productCode);
                    } catch (Exception e) {
                        logger.error("failed to subscribe product: '" + productCode + "'", e);
                        future.fail(e);
                    }
                }, result -> {
                    logger.info("raw result: " + result.result());
                    String status = "failed";
                    if (result.succeeded()) {
                        logger.info("subscription succeeded for " + productCode);
                        status = "registered";
                    } else {
                        logger.info("subscription failed for " + productCode);
                    }
                    final JsonObject reply = new JsonObject().put("status", status);
                    message.reply(reply);
                });
            } else {
                logger.info("already registered: " + productCode);
                final JsonObject reply = new JsonObject().put("status", "already_registered");
                message.reply(reply);
            }
        });
        logger.info("quotes subscription service deployed");
    }

    private static void setupUnsubscribeTick(Vertx vertx) {
        Observable<Message<JsonObject>> consumer =
                vertx.eventBus().<JsonObject>consumer(ADDRESS_UNSUBSCRIBE_TICK).toObservable();
        consumer.subscribe(message -> {
            final JsonObject contract = message.body();
            logger.info("received unsubscription request for: " + contract);
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

    public static void setupContractRetrieve(Vertx vertx, IBrokersMarketDataCallback ibrokersClient) {
        final MessageConsumer<JsonObject> consumer = vertx.eventBus().consumer(ADDRESS_CONTRACT_RETRIEVE);
        Observable<Message<JsonObject>> contractStream = consumer.toObservable();
        contractStream.subscribe(message -> {
            final JsonObject body = message.body();
            logger.info("requesting contract: " + body);
            final int productCode = Integer.parseInt(body.getString("conId"));
            Contract contract = new Contract();
            contract.conid(productCode);
            String errorChannel = ibrokersClient.request(contract, message);
            Observable<JsonObject> errorStream =
                    vertx.eventBus().<JsonObject>consumer(errorChannel).bodyStream().toObservable();
            errorStream.subscribe(errorMessage -> {
                logger.error("failed to request contract '" + productCode + "': " + errorMessage);
                Gson gson = new GsonBuilder().create();
                JsonObject product = new JsonObject(gson.toJson(contract));
                message.reply(createErrorReply(errorMessage, product));
            });
        });
    }

    public static JsonObject createErrorReply(JsonObject errorMessage, JsonObject content) {
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

    private static void setupAdminCommand(Vertx vertx) {
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

    public static ObservableFuture<Message<JsonObject>> createContractStream(Vertx vertx, Integer ibCode) {
        JsonObject product = new JsonObject().put("conId", Integer.toString(ibCode));
        ObservableFuture<Message<JsonObject>> contractStream = RxHelper.observableFuture();
        logger.info("requesting subscription for product: " + ibCode);
        vertx.eventBus().send(MarketDataVerticle.ADDRESS_CONTRACT_RETRIEVE, product, contractStream.toHandler());
        return contractStream;
    }

    public static void adminCommand(Vertx vertx, String commandLine) {
        vertx.eventBus().send(MarketDataVerticle.ADDRESS_ADMIN_COMMAND, commandLine, reply -> {
            if (reply.succeeded()) {
                String commandResult = (String) reply.result().body();
                logger.debug(commandLine + " -> '" + commandResult + "'");
            } else {
                logger.error("failed to run admin command '" + commandLine + "'");
            }
        });
    }

    private static void setupHistoricalEOD(Vertx vertx, IBrokersMarketDataCallback ibrokersClient) {
        final MessageConsumer<JsonObject> consumer = vertx.eventBus().consumer(ADDRESS_EOD_REQUEST);
        Observable<Message<JsonObject>> histRequestStream = consumer.toObservable();
        histRequestStream.subscribe(request -> {
            final JsonObject contractDetails = request.body();
            JsonObject contractJson = contractDetails.getJsonObject("m_contract");
            Integer productCode = contractJson.getInteger("m_conid");;
            logger.info("received historical eod request for: " + productCode);
            String replyAddress = ADDRESS_EOD_DATA_PREFIX + "." + productCode;
            String errorChannel = ibrokersClient.eod(contractDetails, replyAddress);
            if (errorChannel != null) {
                MessageConsumer<JsonObject> errorConsumer = vertx.eventBus().consumer(errorChannel);
                Observable<JsonObject> errorStream = errorConsumer.bodyStream().toObservable();
                errorStream.subscribe(errorMessage -> {
                    logger.error("failed to subscribe for contract '" + productCode + "': " + errorMessage);
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
        logger.info("starting market data");
        String storageDirPathName =
                String.join(File.separator, config().getJsonArray(MarketData.IBROKERS_TICKS_STORAGE_PATH).getList());
        Path storageDirPath = FileSystems.getDefault().getPath(storageDirPathName);

        logger.info("ticks data storage set to '" + storageDirPath + "'");
        String ibrokersHost = config().getString("ibrokers.host");
        Integer ibrokersPort = config().getInteger("ibrokers.port");
        Integer ibrokersClientId = config().getInteger("ibrokers.clientId");
        IBrokersMarketDataCallback ibrokersClient = new IBrokersMarketDataCallback(vertx.eventBus(), storageDirPath);
        vertx.executeBlocking(future -> {
            try {
                org.omarket.trading.ibrokers.Util.ibrokers_connect(ibrokersHost, ibrokersPort, ibrokersClientId,
                        ibrokersClient);
                setupContractRetrieve(vertx, ibrokersClient);
                setupHistoricalEOD(vertx, ibrokersClient);
                setupSubscribeTick(vertx, ibrokersClient);
                setupUnsubscribeTick(vertx);
                setupAdminCommand(vertx);
                future.complete();
            } catch (IBrokersConnectionFailure iBrokersConnectionFailure) {
                logger.error("connection failed", iBrokersConnectionFailure);
                future.fail(iBrokersConnectionFailure);
            }
        }, result -> {
            if (result.succeeded()) {
                logger.info("started market data verticle");
                startFuture.complete();
            } else {
                logger.info("failed to start market data verticle");
                startFuture.fail("failed to start market data verticle");
            }
        });
    }
}
