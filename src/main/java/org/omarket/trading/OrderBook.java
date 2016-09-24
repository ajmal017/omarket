package org.omarket.trading;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.*;

/**
 * Created by christophe on 23.09.16.
 *
 * Depth Updates
 *
 * The final level of market data detail is the inclusion of market depth updates.
 * Depth updates contain a record of every change to every order in the order book for
 * a particular security. Depth data sets can often be limited.
 *
 * Some depth data sets only provide an aggregated price view, the total quantity and number
 * of orders at each price level. Others will only provide the first few price levels.
 *
 * A good depth update data set will contain a record of the following fields every time one of them changes:
 *
 *  - Symbol - Security symbol (e.g. BHP)
 *  - Exchange - Exchange the update is from
 *  - Time - Order update time
 *  - Order Position OR Unique Order Identifier - This identifies which order has been changed (either by relative position in the queue, or by an order Id)
 *  - Quantity - Order quantity
 *  - Update Type - NEW or ENTER (a new order has been entered), UPDATE (volume has been amended) or DELETE (order has been removed)
 *
 * Market depth updates are required for accurately back-testing intraday trading strategies
 * where you are submitting orders that will enter the order book queue rather than executing
 * immediately at the best market price.
 *
 */
public class OrderBook {
    private static Logger logger = LoggerFactory.getLogger(OrderBook.class);

    private TreeMap<BigDecimal, TreeMap<String, Order>> bidSide = new TreeMap<>();
    private TreeMap<BigDecimal, TreeMap<String, Order>> askSide = new TreeMap<>();
    private TreeMap<String, BigDecimal> orderIdToPrice = new TreeMap<>();
    private Date lastUpdate = null;

    public String newEntryFromOrder(Order order, TreeMap<BigDecimal, TreeMap<String, Order>> side){
        BigDecimal price = order.getPrice();
        String orderId = order.getOrderId();
        TreeMap<String, Order> targetPriceGroup = side.get(price);
        if (targetPriceGroup == null){
            targetPriceGroup = new TreeMap<>();
            side.put(order.getPrice(), targetPriceGroup);
        }
        targetPriceGroup.put(orderId, order);
        orderIdToPrice.put(orderId, price);
        this.lastUpdate = order.getTimestamp();
        return orderId;
    }

    public String newBid(BigDecimal price, Integer quantity, String orderId) {
        if(getBestAsk() != null) {assert price.compareTo(getBestAsk().getLeft()) == -1;}
        Order newOrder = new Order(price, quantity, orderId);
        return newEntryFromOrder(newOrder, bidSide);
    }

    public String newBid(BigDecimal price, Integer quantity) {
        if(getBestAsk() != null) {assert price.compareTo(getBestAsk().getLeft()) == -1;}
        Order newOrder = new Order(price, quantity);
        return newEntryFromOrder(newOrder, bidSide);
    }

    public String newAsk(BigDecimal price, Integer quantity, String orderId) {
        if(getBestBid() != null) {assert price.compareTo(getBestBid().getLeft()) == 1;}
        Order newOrder = new Order(price, quantity, orderId);
        return newEntryFromOrder(newOrder, askSide);
    }

    public String newAsk(BigDecimal price, Integer quantity) {
        if(getBestBid() != null) {assert price.compareTo(getBestBid().getLeft()) == 1;}
        Order newOrder = new Order(price, quantity);
        return newEntryFromOrder(newOrder, askSide);
    }
    public void deleteOrder(String orderId) {
        BigDecimal targetPrice = orderIdToPrice.get(orderId);
        TreeMap<String, Order> orders = bidSide.get(targetPrice);
        if (orders != null){
            orders.remove(orderId);
            if(orders.size() == 0){
                bidSide.remove(targetPrice);
            }
        }
        orders = askSide.get(targetPrice);
        if (orders != null){
            orders.remove(orderId);
            if(orders.size() == 0){
                askSide.remove(targetPrice);
            }
        }
        orderIdToPrice.remove(orderId);
    }

    public Date getLastUpdate() {
        return this.lastUpdate;
    }

    public List<Pair<BigDecimal, Integer>> getBidOrderLevels() {
        int counter = 0;
        List<Pair<BigDecimal, Integer>> orders = new LinkedList<>();
        for(BigDecimal price: bidSide.keySet()){
            counter++;
            TreeMap<String, Order> ordersById = bidSide.get(price);
            ImmutablePair<BigDecimal, Integer> ordersAggregate = OrderBook.aggregateOrders(ordersById);
            orders.add(0, ordersAggregate);
        }
        return orders;
    }

    public List<Pair<BigDecimal, Integer>> getAskOrderLevels() {
        int counter = 0;
        List<Pair<BigDecimal, Integer>> orders = new LinkedList<>();
        for(BigDecimal price: askSide.keySet()){
            counter++;
            TreeMap<String, Order> ordersById = askSide.get(price);
            ImmutablePair<BigDecimal, Integer> ordersAggregate = OrderBook.aggregateOrders(ordersById);
            orders.add(ordersAggregate);
        }
        return orders;
    }

    private static ImmutablePair<BigDecimal, Integer> aggregateOrders(TreeMap<String, Order> ordersById) {
        ImmutablePair<BigDecimal, Integer> ordersAggregate;
        if(ordersById.size() == 0){
            return null;
        }
        BigDecimal price = ordersById.firstEntry().getValue().getPrice();
        ordersAggregate = new ImmutablePair<>(price, 0);
        for(Order order: ordersById.values()){
            Integer previousSize = ordersAggregate.getRight();
            ordersAggregate = new ImmutablePair<>(price, previousSize + order.getQuantity());
        }
        return ordersAggregate;
    }

    public Pair<BigDecimal, Integer> getBestBid(){
        if (bidSide.size() == 0){
            return null;
        }
        BigDecimal bestBid = bidSide.lastKey();
        TreeMap<String, Order> ordersById = bidSide.get(bestBid);
        return aggregateOrders(ordersById);
    }

    public Pair<BigDecimal, Integer> getBestAsk(){
        if (askSide.size() == 0){
            return null;
        }
        BigDecimal bestAsk = askSide.firstKey();
        TreeMap<String, Order> ordersById = askSide.get(bestAsk);
        return aggregateOrders(ordersById);
    }

}

