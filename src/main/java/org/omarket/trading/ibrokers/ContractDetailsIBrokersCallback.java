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
public class ContractDetailsIBrokersCallback extends AbstractIBrokersCallback {
    private static Logger logger = LoggerFactory.getLogger(ContractDetailsIBrokersCallback.class);
    private Integer lastRequestId = null;
    private Map<Integer, Message<JsonObject> > callbackMessages = new HashMap<>();

    private Integer newRequestId(){
        if (lastRequestId == null){
            lastRequestId = 0;
        }
        lastRequestId += 1;
        return lastRequestId;
    }

    public void request(Contract contract, Message<JsonObject> message){
        Integer newRequestId = newRequestId();
        callbackMessages.put(newRequestId, message);
        getClient().reqContractDetails(newRequestId, contract);
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

}
