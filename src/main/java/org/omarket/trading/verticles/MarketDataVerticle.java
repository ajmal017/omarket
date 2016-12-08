package org.omarket.trading.verticles;

import com.ib.client.*;
import io.vertx.rx.java.ObservableFuture;
import io.vertx.rx.java.RxHelper;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.eventbus.Message;
import io.vertx.rxjava.core.eventbus.MessageConsumer;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.omarket.trading.ibrokers.*;
import org.omarket.trading.quote.QuoteConverter;
import rx.*;
import rx.Observable;
import rx.Observer;

import java.io.*;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.text.ParseException;
import java.util.*;

import static org.omarket.trading.MarketData.createChannelQuote;

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

    public void start() throws Exception {
        logger.info("starting market data");
        String storageDirPathName = String.join(File.separator, config().getJsonArray(IBROKERS_TICKS_STORAGE_PATH).getList());
        Path storageDirPath = FileSystems.getDefault().getPath(storageDirPathName);

        logger.info("ticks data storage set to '" + storageDirPath + "'");
        String ibrokersHost = config().getString("ibrokers.host");
        Integer ibrokersPort = config().getInteger("ibrokers.port");
        Integer ibrokersClientId = config().getInteger("ibrokers.clientId");

        vertx.executeBlocking(future -> {
            final IBrokersMarketDataCallback ewrapper = new IBrokersMarketDataCallback(vertx.eventBus(), storageDirPath);
            org.omarket.trading.ibrokers.Util.ibrokers_connect(ibrokersHost, ibrokersPort, ibrokersClientId, ewrapper);
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
        Observable<Message<JsonObject>> consumer = vertx.eventBus().<JsonObject>consumer(ADDRESS_SUBSCRIBE_TICK).toObservable();
        consumer.subscribe(message -> {
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

    public static void processContractRetrieve(Vertx vertx) {
        Observable<Message<JsonObject>> contractStream = vertx.eventBus().<JsonObject>consumer(ADDRESS_CONTRACT_RETRIEVE).toObservable();
        contractStream.subscribe(message -> {
            final JsonObject body = message.body();
            logger.info("received: " + body);
            int productCode = Integer.parseInt(body.getString("conId"));
            Contract contract = new Contract();
            contract.conid(productCode);
            ibrokers_client.request(contract, message);
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

    public static void subscribeProduct(Vertx vertx, Integer ibCode, Handler<AsyncResult<Message<JsonObject>>> replyHandler) {
        JsonObject product = new JsonObject().put("conId", Integer.toString(ibCode));
        /*
        ObservableFuture<Message<JsonObject>> observable = RxHelper.observableFuture();
        observable.subscribe(
                result -> {
                    JsonObject contract = result.body();
                    vertx.eventBus().send(MarketDataVerticle.ADDRESS_SUBSCRIBE_TICK, contract, mktDataReply -> {
                                logger.info("subscription result: " + mktDataReply.result());

                        logger.info("subscription succeeded for product: " + ibCode);
                        contracts.put(ibCode, contract);

                        // forwards quotes to strategy processor
                        final String channelProduct = createChannelQuote(ibCode);
                        MessageConsumer<JsonObject> consumer = vertx.eventBus().consumer(channelProduct);
                        consumer.toObservable().subscribe(message -> {
                            try {
                                quote = QuoteConverter.fromJSON(message.body());
                                logger.info("updated quote: " + quote);
                                if (contracts.size() != AbstractStrategyVerticle.this.getIBrokersCodes().length || quote == null) {
                                    return;
                                }
                                logger.info("processing order book: " + quote);
                                AbstractStrategyVerticle.this.processQuote(quote, false);
                                try {
                                    AbstractStrategyVerticle.this.updateQuotes(quote);
                                } catch (ParseException e) {
                                    logger.error("unable to update quotes", e);
                                }
                            } catch (ParseException e) {
                                logger.error("failed to parse tick data for contract " + contract, e);
                            }
                        });
                    });
                },
                failure -> {
                    logger.error("failed to retrieve contract details: ", failure);
                }
        );

        logger.info("subscription received for product: " + ibCode);
        if (subscribeProductResult.succeeded()) {

        } else {
            logger.error("failed to subscribe to: " + ibCode);
        }
        vertx.eventBus().send(MarketDataVerticle.ADDRESS_CONTRACT_RETRIEVE, product, observable.toHandler());
        */

        vertx.eventBus().send(MarketDataVerticle.ADDRESS_CONTRACT_RETRIEVE, product, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> reply) {
                if (reply.succeeded()) {
                    JsonObject contractDetails = reply.result().body();
                    vertx.eventBus().send(MarketDataVerticle.ADDRESS_SUBSCRIBE_TICK, contractDetails, mktDataReply -> {
                        logger.info("subscription result: " + mktDataReply.result());
                    });
                } else {
                    logger.error("failed to retrieve contract details: ", reply.cause());
                }
                if (replyHandler != null) {
                    replyHandler.handle(reply);
                }
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
