package org.omarket.trading;

import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.rx.java.ObservableFuture;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.eventbus.Message;
import org.omarket.trading.verticles.MarketDataVerticle;
import rx.Observable;
import rx.functions.Func1;

/**
 * Created by Christophe on 04/01/2017.
 */

public class ContractFetcher implements Func1<String, Observable<? extends Message<JsonObject>>> {
    private final Vertx vertx;

    public ContractFetcher(Vertx vertx) {
        this.vertx = vertx;
    }

    @Override
    public Observable<? extends Message<JsonObject>> call(String code) {
        JsonObject contractCode = new JsonObject().put("conId", code);
        ObservableFuture<Message<JsonObject>> contractStream =
                io.vertx.rx.java.RxHelper.observableFuture();
        DeliveryOptions deliveryOptions = new DeliveryOptions();
        deliveryOptions.setSendTimeout(10000);
        vertx.eventBus().send(MarketDataVerticle.ADDRESS_CONTRACT_DOWNLOAD, contractCode,
                deliveryOptions, contractStream.toHandler());
        return contractStream;
    }
}