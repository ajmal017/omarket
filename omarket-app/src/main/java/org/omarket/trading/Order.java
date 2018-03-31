package org.omarket.trading;

import java.math.BigDecimal;
import java.util.Date;
import java.util.UUID;

/**
 * Created by christophe on 23.09.16.
 */
public class Order {
    private Date timestamp = null;
    private BigDecimal price = null;
    private Integer quantity = null;
    private String orderId = null;


    public Order(BigDecimal price, Integer quantity, String orderId){
        assert quantity > 0;
        this.timestamp = new Date();
        this.price = price;
        this.quantity = quantity;
        this.orderId = orderId;
    }

    public Order(BigDecimal price, Integer quantity){
        this(price, quantity, UUID.randomUUID().toString());
    }

    @Override
    public String toString() {
        return "Order{ " + quantity +
                " @ " + price +
                " " + orderId + " }";
    }

    public void setQuantity(Integer newQuantity) {
        this.quantity = newQuantity;
        this.timestamp = new Date();
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public Integer getQuantity() {
        return quantity;
    }

    public String getOrderId() {
        return orderId;
    }

}
