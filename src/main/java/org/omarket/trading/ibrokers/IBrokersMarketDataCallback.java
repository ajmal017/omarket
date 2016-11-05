package org.omarket.trading.ibrokers;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.ib.client.Contract;
import com.ib.client.ContractDetails;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.omarket.trading.OrderBookLevelOne;
import org.omarket.trading.verticles.MarketDataVerticle;
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
    private Map<Integer, Pair<OrderBookLevelOne, Contract>> orderBooks = new HashMap<>();
    private EventBus eventBus;
    private final static int PRICE_BID = 1;
    private final static int PRICE_ASK = 2;
    private final static int SIZE_BID = 0;
    private final static int SIZE_ASK = 3;

    public IBrokersMarketDataCallback(EventBus eventBus){
        this.eventBus = eventBus;
    }

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

    public void subscribe(Contract contract, double minTick) {
        int tickerId = newSubscriptionId();
        orderBooks.put(tickerId, new ImmutablePair<>(new OrderBookLevelOne(minTick), contract));
        getClient().reqMktData(tickerId, contract, "", false, null);
    }

    @Override
    public void contractDetails(int requestId, ContractDetails contractDetails) {
        try {
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
        logger.debug("received contract details end for request: {}", currentRequestId);
    }

    @Override
    public void currentTime(long time) {
        logger.info("requested current time: {}", time);
    }

    @Override
    public void tickPrice(int tickerId, int field, double price, int canAutoExecute) {
        if (field != PRICE_BID && field != PRICE_ASK){ return; }
        Pair<OrderBookLevelOne, Contract> orderBookContract = orderBooks.get(tickerId);
        OrderBookLevelOne orderBook = orderBookContract.getLeft();
        if (field == PRICE_BID){
            orderBook.setBestBidPrice(price);
        } else {
            orderBook.setBestAskPrice(price);
        }
        Contract contract = orderBookContract.getRight();
        String channel = MarketDataVerticle.createChannelOrderBookLevelOne(contract.conid());
        this.eventBus.send(channel, orderBook.asJSON());
    }

    @Override
    public void tickSize(int tickerId, int field, int size) {
        if (field != SIZE_BID && field != SIZE_ASK){ return; }
        Pair<OrderBookLevelOne, Contract> orderBookContract = orderBooks.get(tickerId);
        OrderBookLevelOne orderBook = orderBookContract.getLeft();
        if (field == SIZE_BID){
            orderBook.setBestBidSize(size);
        } else {
            orderBook.setBestAskSize(size);
        }
        Contract contract = orderBookContract.getRight();
        String channel = MarketDataVerticle.createChannelOrderBookLevelOne(contract.conid());
        this.eventBus.send(channel, orderBook.asJSON());
    }
}
