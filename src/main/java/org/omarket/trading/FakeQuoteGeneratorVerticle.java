package org.omarket.trading;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Handler;

import java.math.BigDecimal;
import java.util.Random;

public class FakeQuoteGeneratorVerticle extends AbstractVerticle {

    private String channelName = null;
    private Integer period = null;
    private BigDecimal currentValue = null;
    private BigDecimal increment = null;
    private BigDecimal decrement = null;

    public FakeQuoteGeneratorVerticle(String channelName, Integer period, BigDecimal startValue, BigDecimal increment){
        this.channelName = channelName;
        this.period = period;
        this.currentValue = startValue;
        this.increment = increment;
        this.decrement = this.increment.multiply(BigDecimal.valueOf(-1));
    }

    public void start(Future<Void> startFuture) {
        final Random random = new Random();
        final String channelName = this.channelName;
        vertx.setPeriodic(this.period, aLong -> {
            BigDecimal value = random.nextBoolean()? this.increment: this.decrement;
            this.currentValue = this.currentValue.add(value);
            vertx.eventBus().publish(channelName, this.currentValue.toString());
        });
    }
}