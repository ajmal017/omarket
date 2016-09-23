package org.omarket.trading;

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
        Order newOrder = new Order(price, quantity, orderId);
        return newEntryFromOrder(newOrder, bidSide);
    }

    public String newBid(BigDecimal price, Integer quantity) {
        Order newOrder = new Order(price, quantity);
        return newEntryFromOrder(newOrder, bidSide);
    }

    public String newAsk(BigDecimal price, Integer quantity, String orderId) {
        Order newOrder = new Order(price, quantity, orderId);
        return newEntryFromOrder(newOrder, askSide);
    }

    public String newAsk(BigDecimal price, Integer quantity) {
        Order newOrder = new Order(price, quantity);
        return newEntryFromOrder(newOrder, askSide);
    }
    public void deleteOrder(String orderId) {
        BigDecimal targetPrice = orderIdToPrice.get(orderId);
        TreeMap<String, Order> orders = bidSide.get(targetPrice);
        if (orders != null){
            orders.remove(orderId);
        }
        orders = askSide.get(targetPrice);
        if (orders != null){
            orders.remove(orderId);
        }
    }

    public Date getLastUpdate() {
        return this.lastUpdate;
    }

    public TreeMap<BigDecimal, TreeMap<String, Order>> getBidSide() {
        return bidSide;
    }

    public TreeMap<BigDecimal, TreeMap<String, Order>> getAskSide() {
        return askSide;
    }

}

