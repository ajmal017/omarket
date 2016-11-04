package org.omarket.trading.verticles;

import com.ib.client.*;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.omarket.trading.ibrokers.ContractDetailsIBrokersCallback;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Christophe on 01/11/2016.
 */
public class MarketDataVerticle extends AbstractVerticle{
    Logger logger = LoggerFactory.getLogger(MarketDataVerticle.class.getName());

    public static final String ADDRESS_SUBSCRIBE = "oot.marketData.subscribe";
    public static final String ADDRESS_SUBSCRIBE_MULTIPLE = "oot.marketData.subscribeMultiple";
    public static final String ADDRESS_UNSUBSCRIBE = "oot.marketData.unsubscribe";
    public static final String ADDRESS_UNSUBSCRIBE_MULTIPLE = "oot.marketData.unsubscribeMultiple";
    public static final String ADDRESS_UNSUBSCRIBE_ALL = "oot.marketData.unsubscribeAll";
    public static final String ADDRESS_CONTRACT_DETAILS = "oot.marketData.contractDetails";
    public static final String ADDRESS_CONTRACT_DETAILS_COMPLETED = "oot.marketData.contractDetailsCompleted";

    private Map<String, JsonObject> subscribedProducts = new HashMap<>();

    public void start() {
        logger.info("starting market data");
        final EReaderSignal readerSignal = new EJavaSignal();
        final ContractDetailsIBrokersCallback ewrapper = new ContractDetailsIBrokersCallback();
        final EClientSocket clientSocket = new EClientSocket(ewrapper, readerSignal);
        ewrapper.setClient(clientSocket);
        String ibrokersHost = config().getString("ibrokers.host");
        Integer ibrokersPort = config().getInteger("ibrokers.port");
        Integer ibrokersClientId = config().getInteger("ibrokers.clientId");
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
                        logger.info("IBrokers thread waiting for signal");
                        reader.processMsgs();
                    } catch (Exception e) {
                        logger.error("Exception", e);
                    }
                }
                if(clientSocket.isConnected()){
                    clientSocket.eDisconnect();
                }
            }
        }.start();

        logger.info("starting market data verticle");

        MessageConsumer<JsonObject> consumerSubscribe = vertx.eventBus().consumer(ADDRESS_SUBSCRIBE);
        consumerSubscribe.handler(message -> {
            final JsonObject contractDetails = message.body();
            logger.info("received subscription request for: " + contractDetails);
            String status = "failed";
            int productCode = contractDetails.getJsonObject("m_contract").getInteger("m_conid");
            if(!subscribedProducts.containsKey(Integer.toString(productCode))){
                // subscription takes place here
                subscribedProducts.put(Integer.toString(productCode), contractDetails);
                status = "registered";
            } else {
                status = "already_registered";
            }
            final JsonObject reply = new JsonObject().put("status", status);
            message.reply(reply);
        });

        MessageConsumer<JsonObject> consumerUnsubscribe = vertx.eventBus().consumer(ADDRESS_UNSUBSCRIBE);
        consumerUnsubscribe.handler(message -> {
            final JsonObject contract = message.body();
            logger.info("received unsubscription request for: " + contract);
            String status = "failed";
            String productCode = contract.getString("conId");
            if(subscribedProducts.containsKey(productCode)){
                // un-subscription takes place here
                subscribedProducts.remove(productCode);
                status = "unsubscribed";
            } else {
                status = "missing";
            }
            final JsonObject reply = new JsonObject().put("status", status);
            message.reply(reply);
        });
        logger.info("started market data verticle");

        MessageConsumer<JsonObject> consumer = vertx.eventBus().consumer(ADDRESS_CONTRACT_DETAILS);
        consumer.handler(message -> {
            final JsonObject body = message.body();
            logger.info("received: " + body);
            int productCode = Integer.parseInt(body.getString("conId"));
            Contract contract = new Contract();
            contract.conid(productCode);
            ewrapper.request(contract, message);
        });
    }
}
