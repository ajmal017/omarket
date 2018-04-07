package org.omarket.trading;

import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.rx.java.ObservableFuture;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.eventbus.Message;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import rx.Observable;
import rx.functions.Func1;

/**
 * Created by Christophe on 04/01/2017.
 */
public class ContractFetcher implements Func1<String, Observable<? extends Message<JsonObject>>> {


    private final String addressContractDownload;
    private final Vertx vertx;

    public ContractFetcher(Vertx vertx, String addressContractDownload) {
        this.addressContractDownload = addressContractDownload;
        this.vertx = vertx;
    }

    @Override
    public Observable<? extends Message<JsonObject>> call(String code) {
        JsonObject contractCode = new JsonObject().put("conId", code);
        ObservableFuture<Message<JsonObject>> contractStream =
                io.vertx.rx.java.RxHelper.observableFuture();
        DeliveryOptions deliveryOptions = new DeliveryOptions();
        deliveryOptions.setSendTimeout(10000);
        vertx.eventBus().send(addressContractDownload, contractCode,
                deliveryOptions, contractStream.toHandler());
        return contractStream;
    }
}