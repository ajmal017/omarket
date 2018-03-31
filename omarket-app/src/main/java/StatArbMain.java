import io.vertx.core.DeploymentOptions;
import io.vertx.core.Verticle;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.RxHelper;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.omarket.trading.verticles.DummyMeanReversionStrategyVerticle;
import org.omarket.trading.verticles.HistoricalDataVerticle;
import org.omarket.trading.verticles.VerticleProperties;

public class StatArbMain {
    private final static Logger logger = LoggerFactory.getLogger(StatArbMain.class);

    public static void main(String[] args) throws InterruptedException {
        int defaultClientId = 1;
        final Vertx vertx = Vertx.vertx();

        Verticle historicalDataVerticle = new HistoricalDataVerticle();
        Verticle singleLegMeanReversionStrategyVerticle = new DummyMeanReversionStrategyVerticle();

        DeploymentOptions options = VerticleProperties.makeDeploymentOptions(defaultClientId);
        RxHelper.deployVerticle(vertx, historicalDataVerticle, options)
                .subscribe(historicalDataId -> {
                    logger.info("historical data verticle deployed as " + historicalDataId);
                    RxHelper.deployVerticle(vertx, singleLegMeanReversionStrategyVerticle, options)
                            .doOnNext(strategyId -> {
                                logger.info("strategy verticle deployed as " + strategyId);
                            })
                            .doOnError(logger::error);
                })
                ;
    }

}
