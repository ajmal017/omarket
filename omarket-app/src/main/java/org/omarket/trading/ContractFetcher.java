package org.omarket.trading;

import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.rx.java.ObservableFuture;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.eventbus.Message;
import org.omarket.trading.verticles.MarketDataVerticle;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import rx.Observable;
import rx.functions.Func1;

/**
 * Created by Christophe on 04/01/2017.
 */
@Component
public class ContractFetcher implements Func1<String, Observable<? extends Message<JsonObject>>> {

    @Value("address.contract_download")	private String ADDRESS_CONTRACT_DOWNLOAD;

    @Override
    public Observable<? extends Message<JsonObject>> call(String code) {
        JsonObject contractCode = new JsonObject().put("conId", code);
        ObservableFuture<Message<JsonObject>> contractStream =
                io.vertx.rx.java.RxHelper.observableFuture();
        DeliveryOptions deliveryOptions = new DeliveryOptions();
        deliveryOptions.setSendTimeout(10000);
        final Vertx vertx = Vertx.vertx();
        vertx.eventBus().send(ADDRESS_CONTRACT_DOWNLOAD, contractCode,
                deliveryOptions, contractStream.toHandler());
        return contractStream;
    }
}