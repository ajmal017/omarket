package org.omarket;

import io.vertx.core.json.JsonObject;
import io.vertx.rx.java.ObservableFuture;
import io.vertx.rxjava.core.RxHelper;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.eventbus.Message;
import lombok.extern.slf4j.Slf4j;
import org.omarket.trading.verticles.MarketDataVerticle;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import rx.Observable;

@Slf4j
@Component
public class RecorderService {

    @Autowired
    private MarketDataVerticle marketDataVerticle;

    public void record() {

        final Vertx vertx = Vertx.vertx();

        Observable<String> marketDataDeployment = RxHelper.deployVerticle(vertx, marketDataVerticle);
        String[] ibCodes = new String[]{
                "70083656",
                "59198014",
                "45555861",
                "47649504",
                "2586548",
                "42922245",
                "10753248",
                "239691934",
                "8991557",
                "71423629",
                "136155102",
                "43645828",
                "43240070",
                "39046954",
                "8991562",
                "40369373",
                "12340041",
                "40413278",
                "2586603",
                "96079929",
                "2586543",
                "225312863",
                "47194279",
                "49284274",
                "115826907",
                "95501103",
                "25985141",
                "45540769",
                "27684070",
                "73128548",
                "8991352",
                "41037032",
                "195685436",
                "2586566",
                "31230302",
                "45540699",
                "4215200",
                "84894205",
                "4215215",
                "45540784",
                "15547816",
                "45540828",
                "39832875",
                "115826951",
                "51529211",
                "242140455",
                "45540822",
                "39039301",
                "10209369",
                "4215205",
                "78365384",
                "45444192",
                "4215230",
                "32237620",
                "229726228",
                "4215227",
                "47549883",
                "43652089",
                "4215217",
                "45540782",
                "27684033",
                "4215210",
                "4215235",
                "31421120",
                "10753244",
                "13002510",
                "102194440",
                "229726197",
                "242500577",
                "9579970",
                "38590758",
                "202915575",
                "6604766",
                "229726316",
                "756733",
                "4215220"
        };
        marketDataDeployment.subscribe(deploymentId -> {
            log.info("succesfully deployed market data verticle: " + deploymentId);
            for(String ibCode: ibCodes){
                log.info("subscribing ibCode: " + ibCode);
                JsonObject contract = new JsonObject().put("conId", ibCode);
                ObservableFuture<Message<JsonObject>> contractStream = io.vertx.rx.java.RxHelper.observableFuture();
                vertx.eventBus().send(MarketDataVerticle.ADDRESS_CONTRACT_RETRIEVE, contract, contractStream.toHandler());
                contractStream.subscribe((Message<JsonObject> contractMessage) -> {
                    JsonObject envelopJson = contractMessage.body();
                    if(!envelopJson.getJsonObject("error").isEmpty()){
                        return;
                    }
                    JsonObject contractJson = envelopJson.getJsonObject("content");
                    log.info("contract retrieved: " + contractJson);
                    ObservableFuture<Message<JsonObject>> quoteStream = io.vertx.rx.java.RxHelper.observableFuture();
                    vertx.eventBus().send(MarketDataVerticle.ADDRESS_SUBSCRIBE_TICK, contractJson, quoteStream.toHandler());
                    quoteStream.subscribe(resultMessage -> {
                        JsonObject result = resultMessage.body();
                        log.info("received: " + result);
                    }, err -> {
                        log.error("error", err);
                    });
                });
            }
        });
    }
}
