package org.omarket.trading;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.math.BigDecimal;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;

/**
 * Created by christophe on 23.09.16.
 * <p>
 * Depth Updates.
 * <p>
 * The final level of market data detail is the inclusion of market depth updates.
 * Depth updates contain a record of every change to every order in the order book for
 * a particular security. Depth data sets can often be limited.
 * <p>
 * Some depth data sets only provide an aggregated price view, the total quantity and number
 * of orders at each price level. Others will only provide the first few price levels.
 * <p>
 * A good depth update data set will contain a record of the following fields every time one of them changes:
 * <p>
 * - Symbol - Security symbol (e.g. BHP)
 * - Exchange - Exchange the update is from
 * - Time - Order update time
 * - Order Position OR Unique Order Identifier - This identifies which order has been changed (either by relative position in the queue, or by an order Id)
 * - Quantity - Order quantity
 * - Update Type - NEW or ENTER (a new order has been entered), UPDATE (volume has been amended) or DELETE (order has been removed)
 * <p>
 * Market depth updates are required for accurately back-testing intraday trading strategies
 * where you are submitting orders that will enter the order book queue rather than executing
 * immediately at the best market price.
 */
public class OrderBook {

    private TreeMap<BigDecimal, TreeMap<String, Order>> bidSide = new TreeMap<>();
    private TreeMap<BigDecimal, TreeMap<String, Order>> askSide = new TreeMap<>();
    private TreeMap<String, BigDecimal> orderIdToPrice = new TreeMap<>();
    private Date lastUpdate = null;
    private boolean ignoreInconsistentBidAsk = false;

    private static ImmutablePair<BigDecimal, Integer> aggregateOrders(TreeMap<String, Order> ordersById) {
        ImmutablePair<BigDecimal, Integer> ordersAggregate;
        if (ordersById.size() == 0) {
            return null;
        }
        BigDecimal price = ordersById.firstEntry().getValue().getPrice();
        ordersAggregate = new ImmutablePair<>(price, 0);
        for (Order order : ordersById.values()) {
            Integer previousSize = ordersAggregate.getRight();
            ordersAggregate = new ImmutablePair<>(price, previousSize + order.getQuantity());
        }
        return ordersAggregate;
    }

    public void setIgnoreInconsistentBidAsk(boolean ignoreInconsistentBidAsk) {
        this.ignoreInconsistentBidAsk = ignoreInconsistentBidAsk;
    }

    private String newEntryFromOrder(Order order, TreeMap<BigDecimal, TreeMap<String, Order>> side) {
        BigDecimal price = order.getPrice();
        String orderId = order.getOrderId();
        TreeMap<String, Order> targetPriceGroup = side.get(price);
        if (targetPriceGroup == null) {
            targetPriceGroup = new TreeMap<>();
            side.put(order.getPrice(), targetPriceGroup);
        }
        targetPriceGroup.put(orderId, order);
        orderIdToPrice.put(orderId, price);
        this.lastUpdate = order.getTimestamp();
        return orderId;
    }

    private String newBidEntryFromOrder(Order newOrder) {
        if (getBestAsk() != null) {
            boolean bidLowerThanAsk = newOrder.getPrice().compareTo(getBestAsk()) == -1;
            if (!bidLowerThanAsk && ignoreInconsistentBidAsk) {
                return null;
            } else {
                assert bidLowerThanAsk;
            }
        }
        return newEntryFromOrder(newOrder, bidSide);
    }

    private String newAskEntryFromOrder(Order newOrder) {
        if (getBestBid() != null) {
            boolean askHigherThanBid = newOrder.getPrice().compareTo(getBestBid()) == 1;
            if (!askHigherThanBid && ignoreInconsistentBidAsk) {
                return null;
            } else {
                assert askHigherThanBid;
            }
        }
        return newEntryFromOrder(newOrder, askSide);
    }

    public String newBid(BigDecimal price, Integer quantity, String orderId) {
        Order newOrder = new Order(price, quantity, orderId);
        return newBidEntryFromOrder(newOrder);
    }

    public String newBid(BigDecimal price, Integer quantity) {
        Order newOrder = new Order(price, quantity);
        return newBidEntryFromOrder(newOrder);
    }

    public String newAsk(BigDecimal price, Integer quantity, String orderId) {
        Order newOrder = new Order(price, quantity, orderId);
        return newAskEntryFromOrder(newOrder);
    }

    public String newAsk(BigDecimal price, Integer quantity) {
        Order newOrder = new Order(price, quantity);
        return newAskEntryFromOrder(newOrder);
    }

    public void deleteOrder(String orderId) {
        BigDecimal targetPrice = orderIdToPrice.get(orderId);
        TreeMap<String, Order> orders = bidSide.get(targetPrice);
        if (orders != null) {
            orders.remove(orderId);
            if (orders.size() == 0) {
                bidSide.remove(targetPrice);
            }
        }
        orders = askSide.get(targetPrice);
        if (orders != null) {
            orders.remove(orderId);
            if (orders.size() == 0) {
                askSide.remove(targetPrice);
            }
        }
        orderIdToPrice.remove(orderId);
        this.lastUpdate = new Date();
    }

    public void updateOrder(String orderId, Integer newVolume) {
        assert newVolume > 0;
        Order targetOrder = null;
        BigDecimal targetPrice = orderIdToPrice.get(orderId);
        TreeMap<String, Order> orders = bidSide.get(targetPrice);
        if (orders != null) {
            targetOrder = orders.get(orderId);
            targetOrder.setQuantity(newVolume);
        }
        orders = askSide.get(targetPrice);
        if (orders != null) {
            targetOrder = orders.get(orderId);
            targetOrder.setQuantity(newVolume);
        }
        this.lastUpdate = targetOrder.getTimestamp();
    }

    public Date getLastUpdate() {
        return this.lastUpdate;
    }

    public List<Pair<BigDecimal, Integer>> getBidOrderLevels() {
        List<Pair<BigDecimal, Integer>> orders = new LinkedList<>();
        for (BigDecimal price : bidSide.keySet()) {
            TreeMap<String, Order> ordersById = bidSide.get(price);
            ImmutablePair<BigDecimal, Integer> ordersAggregate = OrderBook.aggregateOrders(ordersById);
            orders.add(0, ordersAggregate);
        }
        return orders;
    }

    public List<Pair<BigDecimal, Integer>> getAskOrderLevels() {
        List<Pair<BigDecimal, Integer>> orders = new LinkedList<>();
        for (BigDecimal price : askSide.keySet()) {
            TreeMap<String, Order> ordersById = askSide.get(price);
            ImmutablePair<BigDecimal, Integer> ordersAggregate = OrderBook.aggregateOrders(ordersById);
            orders.add(ordersAggregate);
        }
        return orders;
    }

    public BigDecimal getBestBid() {
        Pair<BigDecimal, Integer> best = getBidLevel(0);
        if (best == null) {
            return null;
        }
        return best.getLeft();
    }

    public BigDecimal getBestAsk() {
        Pair<BigDecimal, Integer> best = getAskLevel(0);
        if (best == null) {
            return null;
        }
        return best.getLeft();
    }

    public Pair<BigDecimal, Integer> getAskLevel(Integer level) {
        assert level >= 0;
        if (askSide.size() == 0) {
            return null;
        }
        Iterator<BigDecimal> keyIterator = askSide.navigableKeySet().iterator();
        int counter = 0;
        BigDecimal bestPrice = null;
        while (keyIterator.hasNext()) {
            bestPrice = keyIterator.next();
            counter++;
            if (counter > level) {
                break;
            }
        }
        if (counter <= level) {
            return null;
        }
        TreeMap<String, Order> ordersById = askSide.get(bestPrice);
        Pair<BigDecimal, Integer> best = aggregateOrders(ordersById);
        if (best == null) {
            return null;
        }
        return best;
    }

    public Pair<BigDecimal, Integer> getBidLevel(Integer level) {
        assert level >= 0;
        if (bidSide.size() == 0) {
            return null;
        }
        Iterator<BigDecimal> keyIterator = bidSide.descendingKeySet().iterator();
        int counter = 0;
        BigDecimal bestPrice = null;
        while (keyIterator.hasNext()) {
            bestPrice = keyIterator.next();
            counter++;
            if (counter > level) {
                break;
            }
        }
        if (counter <= level) {
            return null;
        }
        TreeMap<String, Order> ordersById = bidSide.get(bestPrice);
        Pair<BigDecimal, Integer> best = aggregateOrders(ordersById);
        if (best == null) {
            return null;
        }
        return best;
    }

}

