package org.omarket.trading.verticles;

import com.ib.client.*;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.omarket.trading.ibrokers.IBrokersMarketDataCallback;

import java.io.*;
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
    public static final String IBROKERS_TICKS_STORAGE_PATH = "ibrokers.ticks.storagePath";
    private static IBrokersMarketDataCallback ibrokers_client;
    private final static Map<String, JsonObject> subscribedProducts = new HashMap<>();

    private static String getProductAsString(String ibCode){
        JsonObject product = subscribedProducts.get(ibCode);
        if (product == null){
            logger.error("product not found for : " + ibCode);
            return null;
        }
        return product.toString();
    }
    private static Set<String> getSubscribedProducts(){
        return subscribedProducts.keySet();
    }

    static public String createChannelOrderBookLevelOne(Integer ibCode) {
        return ADDRESS_ORDER_BOOK_LEVEL_ONE + "." + ibCode;
    }

    static private IBrokersMarketDataCallback ibrokers_connect(String ibrokersHost, int ibrokersPort, int ibrokersClientId, EventBus eventBus, Path storageDirPath) {
        final EReaderSignal readerSignal = new EJavaSignal();
        final IBrokersMarketDataCallback ewrapper = new IBrokersMarketDataCallback(eventBus, storageDirPath);
        final EClientSocket clientSocket = new EClientSocket(ewrapper, readerSignal);
        ewrapper.setClient(clientSocket);
        clientSocket.eConnect(ibrokersHost, ibrokersPort, ibrokersClientId);

        /*
        Launching IBrokers client thread
         */
        new Thread() {
            public void run() {
                EReader reader = new EReader(clientSocket, readerSignal);
                reader.start();
                while (clientSocket.isConnected()) {
                    readerSignal.waitForSignal();
                    try {
                        logger.debug("IBrokers thread waiting for signal");
                        reader.processMsgs();
                    } catch (Exception e) {
                        logger.error("Exception", e);
                    }
                }
                if (clientSocket.isConnected()) {
                    clientSocket.eDisconnect();
                }
            }
        }.start();
        return ewrapper;
    }

    public void start() throws Exception {
        logger.info("starting market data");
        String storageDirPathName = String.join(File.separator, config().getJsonArray(IBROKERS_TICKS_STORAGE_PATH).getList());
        Path storageDirPath = FileSystems.getDefault().getPath(storageDirPathName);

        logger.info("ticks data storage set to '" + storageDirPath + "'");
        String ibrokersHost = config().getString("ibrokers.host");
        Integer ibrokersPort = config().getInteger("ibrokers.port");
        Integer ibrokersClientId = config().getInteger("ibrokers.clientId");

        vertx.executeBlocking(future -> {
            ibrokers_client = ibrokers_connect(ibrokersHost, ibrokersPort, ibrokersClientId, vertx.eventBus(), storageDirPath);
            logger.info("starting market data verticle");
            processContractRetrieve(vertx);
            processSubscribeTick(vertx);
            processUnsubscribeTick(vertx);
            processAdminCommand(vertx);
            future.succeeded();
        }, result -> {
            if (result.succeeded()) {
                logger.info("started market data verticle");
            } else {
                logger.info("failed to start market data verticle");
            }
        }
        );
    }

    private static void processSubscribeTick(Vertx vertx) {
        MessageConsumer<JsonObject> consumer = vertx.eventBus().consumer(ADDRESS_SUBSCRIBE_TICK);
        consumer.handler(message -> {
            final JsonObject contractDetails = message.body();
            logger.info("received subscription request for: " + contractDetails);
            String status = "failed";
            JsonObject contract_json = contractDetails.getJsonObject("m_contract");
            int productCode = contract_json.getInteger("m_conid");
            if (!subscribedProducts.containsKey(Integer.toString(productCode))) {
                Contract contract = new Contract();
                contract.conid(productCode);
                contract.currency(contract_json.getString("m_currency"));
                contract.exchange(contract_json.getString("m_exchange"));
                contract.secType(contract_json.getString("m_sectype"));
                try {
                    ibrokers_client.subscribe(contract, contractDetails.getDouble("m_minTick"));
                    subscribedProducts.put(Integer.toString(productCode), contractDetails);
                    status = "registered";
                } catch (IOException e) {
                    logger.error("failed to subscribe product: '" + productCode + "'", e);
                }
            } else {
                status = "already_registered";
            }
            final JsonObject reply = new JsonObject().put("status", status);
            message.reply(reply);
        });
    }

    private static void processUnsubscribeTick(Vertx vertx) {
        MessageConsumer<JsonObject> consumer = vertx.eventBus().consumer(ADDRESS_UNSUBSCRIBE_TICK);
        consumer.handler(message -> {
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

    private static void processContractRetrieve(Vertx vertx) {
        MessageConsumer<JsonObject> consumer = vertx.eventBus().consumer(ADDRESS_CONTRACT_RETRIEVE);
        consumer.handler(message -> {
            final JsonObject body = message.body();
            logger.info("received: " + body);
            int productCode = Integer.parseInt(body.getString("conId"));
            Contract contract = new Contract();
            contract.conid(productCode);
            ibrokers_client.request(contract, message);
        });
    }

    private static void processAdminCommand(Vertx vertx) {
        MessageConsumer<String> consumer = vertx.eventBus().consumer(ADDRESS_ADMIN_COMMAND);
        consumer.handler(message -> {
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

    public static void subscribeProduct(Vertx vertx, Integer ibCode){
        subscribeProduct(vertx, ibCode, null);
    }

    public static void subscribeProduct(Vertx vertx, Integer ibCode, Handler<AsyncResult<Message<JsonObject>>> replyHandler) {
        JsonObject product = new JsonObject().put("conId", Integer.toString(ibCode));
        vertx.eventBus().send(MarketDataVerticle.ADDRESS_CONTRACT_RETRIEVE, product, (AsyncResult<Message<JsonObject>>reply) -> {
            if (reply.succeeded()) {
                JsonObject contractDetails = reply.result().body();
                vertx.eventBus().send(MarketDataVerticle.ADDRESS_SUBSCRIBE_TICK, contractDetails, mktDataReply -> {
                    logger.info("subscription result: " + mktDataReply.result().body());
                });
            } else {
                logger.error("failed to retrieve contract details: ", reply.cause());
            }
            if(replyHandler != null) {
                replyHandler.handle(reply);
            }
        });
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
