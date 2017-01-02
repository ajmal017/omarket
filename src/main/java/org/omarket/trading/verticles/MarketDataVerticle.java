package org.omarket.trading.verticles;

import com.ib.client.*;
import io.vertx.core.Future;
import io.vertx.rx.java.ObservableFuture;
import io.vertx.rx.java.RxHelper;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.omarket.trading.MarketData;
import org.omarket.trading.ibrokers.*;
import rx.Observable;

import java.io.*;
import java.math.BigDecimal;
import java.math.MathContext;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.*;

/**
 * Created by Christophe on 01/11/2016.
 */
public class MarketDataVerticle extends AbstractVerticle {
    private final static Logger logger = LoggerFactory.getLogger(MarketDataVerticle.class.getName());
    public final static String ADDRESS_SUBSCRIBE_TICK = "oot.marketData.subscribeTick";
    public final static String ADDRESS_UNSUBSCRIBE_TICK = "oot.marketData.unsubscribeTick";
    public final static String ADDRESS_UNSUBSCRIBE_ALL = "oot.marketData.unsubscribeAll";
    public final static String ADDRESS_CONTRACT_RETRIEVE = "oot.marketData.contractRetrieve";
    public final static String ADDRESS_ORDER_BOOK_LEVEL_ONE = "oot.marketData.orderBookLevelOne";
    public final static String ADDRESS_ADMIN_COMMAND = "oot.marketData.adminCommand";
    public final static String ADDRESS_ERROR_MESSAGE_PREFIX = "oot.marketData.error";
    private final static Map<String, JsonObject> subscribedProducts = new HashMap<>();

    public static String getErrorChannel(Integer requestId){
        return ADDRESS_ERROR_MESSAGE_PREFIX + "." + requestId;
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

    public void start(Future<Void> startFuture) throws Exception {
        logger.info("starting market data");
        String storageDirPathName = String.join(File.separator, config().getJsonArray(MarketData.IBROKERS_TICKS_STORAGE_PATH).getList());
        Path storageDirPath = FileSystems.getDefault().getPath(storageDirPathName);

        logger.info("ticks data storage set to '" + storageDirPath + "'");
        String ibrokersHost = config().getString("ibrokers.host");
        Integer ibrokersPort = config().getInteger("ibrokers.port");
        Integer ibrokersClientId = config().getInteger("ibrokers.clientId");
        IBrokersMarketDataCallback ibrokersClient = new IBrokersMarketDataCallback(vertx.eventBus(), storageDirPath);
        vertx.executeBlocking(future -> {
                    try {
                        org.omarket.trading.ibrokers.Util.ibrokers_connect(ibrokersHost, ibrokersPort, ibrokersClientId, ibrokersClient);
                        processContractRetrieve(vertx, ibrokersClient);
                        processSubscribeTick(vertx, ibrokersClient);
                        processUnsubscribeTick(vertx);
                        processAdminCommand(vertx);
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
                }
        );
    }

    private static void processSubscribeTick(Vertx vertx, IBrokersMarketDataCallback ibrokersClient) {
        Observable<Message<JsonObject>> consumer = vertx.eventBus().<JsonObject>consumer(ADDRESS_SUBSCRIBE_TICK).toObservable();
        consumer.subscribe(message -> {
            final JsonObject contractDetails = message.body();
            logger.info("received subscription request for: " + contractDetails);
            JsonObject contractJson = contractDetails.getJsonObject("m_contract");
            Integer productCode = contractJson.getInteger("m_conid");
            if (!subscribedProducts.containsKey(Integer.toString(productCode))) {
                vertx.executeBlocking(future -> {
                    try {
                        logger.info("subscribing: " + productCode.toString());
                        Double minTick = contractDetails.getDouble("m_minTick");
                        subscribedProducts.put(Integer.toString(productCode), contractDetails);
                        String errorChannel = ibrokersClient.subscribe(contractDetails, new BigDecimal(minTick, MathContext.DECIMAL32).stripTrailingZeros());
                        if (errorChannel != null){
                            Observable<JsonObject> errorStream =
                                    vertx.eventBus().<JsonObject>consumer(errorChannel).bodyStream().toObservable();
                            errorStream.subscribe(errorMessage -> {
                                logger.error("failed to subscribe for contract '" + productCode +"': " + errorMessage);
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

    private static void processUnsubscribeTick(Vertx vertx) {
        Observable<Message<JsonObject>> consumer = vertx.eventBus().<JsonObject>consumer(ADDRESS_UNSUBSCRIBE_TICK).toObservable();
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

    public static void processContractRetrieve(Vertx vertx, IBrokersMarketDataCallback ibrokersClient) {
        Observable<Message<JsonObject>> contractStream = vertx.eventBus().<JsonObject>consumer(ADDRESS_CONTRACT_RETRIEVE).toObservable();
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
                logger.error("failed to request contract '" + productCode +"': " + errorMessage);
            });
        });
    }

    private static void processAdminCommand(Vertx vertx) {
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
}
