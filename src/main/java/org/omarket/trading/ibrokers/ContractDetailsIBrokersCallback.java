package org.omarket.trading.ibrokers;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.ib.client.Contract;
import com.ib.client.ContractDetails;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
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
    private Map<Integer, Contract> requests = new HashMap<>();
    private Map<Integer, ContractDetails> replies = new HashMap<>();
    private Integer lastRequestId = null;
    private Message<JsonObject> message;

    public ContractDetailsIBrokersCallback() {
    }

    private Integer newRequestId(){
        if (lastRequestId == null){
            lastRequestId = 0;
        }
        lastRequestId += 1;
        return lastRequestId;
    }

    public void addRequest(Contract contract){
        Integer newRequestId = newRequestId();
        requests.put(newRequestId, contract);
    }

    public void processRequests() {
        for (Integer requestId: requests.keySet()){
            Contract contract = requests.get(requestId);
            getClient().reqContractDetails(requestId, contract);
        }
    }

    @Override
    public void contractDetails(int requestId, ContractDetails contractDetails) {
        try {
            logger.info("received contract details ({}): {}", requestId, contractDetails);
            replies.put(requestId, contractDetails);
        } catch (Exception e) {
            logger.error(e.toString());
        }
    }

    @Override
    public void contractDetailsEnd(int currentRequestId) {
        logger.info("received contract details end for request: {}", currentRequestId);
        requests.remove(currentRequestId);
        if (requests.isEmpty()){
            logger.info("No more request to be processed");
            JsonArray products = new JsonArray();
            for(int requestId: replies.keySet()){
                ContractDetails details = replies.get(requestId);
                Gson gson = new GsonBuilder().create();
                JsonObject product = new JsonObject(gson.toJson(details));
                products.add(product);
            }
            message.reply(products);
            replies.clear();
        }
    }

    @Override
    public void currentTime(long time) {
        logger.info("requested current time: {}", time);
    }


    public void useMessage(Message<JsonObject> message) {
        this.message = message;
    }
}
