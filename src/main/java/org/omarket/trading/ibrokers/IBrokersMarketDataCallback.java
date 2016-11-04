package org.omarket.trading.ibrokers;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.ib.client.Contract;
import com.ib.client.ContractDetails;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;


/**
 * Created by Christophe on 03/11/2016.
 */
public class IBrokersMarketDataCallback extends AbstractIBrokersCallback {
    private static Logger logger = LoggerFactory.getLogger(IBrokersMarketDataCallback.class);
    private Integer lastRequestId = null;
    private Integer lastSubscriptionId = null;
    private Map<Integer, Message<JsonObject>> callbackMessages = new HashMap<>();

    private Integer newRequestId() {
        if (lastRequestId == null) {
            lastRequestId = 0;
        }
        lastRequestId += 1;
        return lastRequestId;
    }

    public void request(Contract contract, Message<JsonObject> message) {
        Integer newRequestId = newRequestId();
        callbackMessages.put(newRequestId, message);
        getClient().reqContractDetails(newRequestId, contract);
    }

    private Integer newSubscriptionId() {
        if (lastSubscriptionId == null) {
            lastSubscriptionId = 0;
        }
        lastSubscriptionId += 1;
        return lastSubscriptionId;
    }

    public void subscribe(Contract contract) {
        int ticketId = newSubscriptionId();
        getClient().reqMktData(ticketId, contract, "", false, null);
    }

    @Override
    public void contractDetails(int requestId, ContractDetails contractDetails) {
        try {
            logger.info("received contract details ({}): {}", requestId, contractDetails);
            Gson gson = new GsonBuilder().create();
            JsonObject product = new JsonObject(gson.toJson(contractDetails));
            Message<JsonObject> message = callbackMessages.get(requestId);
            message.reply(product);
        } catch (Exception e) {
            logger.error(e.toString());
        }
    }

    @Override
    public void contractDetailsEnd(int currentRequestId) {
        logger.info("received contract details end for request: {}", currentRequestId);
    }

    @Override
    public void currentTime(long time) {
        logger.info("requested current time: {}", time);
    }

    @Override
    public void tickPrice(int tickerId, int field, double price, int canAutoExecute) {
        logger.info("tick price: " + tickerId + " " + field + " " + price);
    }

    @Override
    public void tickSize(int tickerId, int field, int size) {
        logger.info("tick size: " + tickerId + " " + field + " " + size);
    }
}
