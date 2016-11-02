/**
 * Created by Christophe on 02/11/2016.
 */
import io.vertx.blog.first.OtherVerticle;
import io.vertx.core.Vertx;

public class Scratchpad2 {

    public static void main(String[] args) throws Exception {
        Vertx vertx = Vertx.vertx();
        System.out.println("Main verticle has started, let's deploy some others...");
        // Different ways of deploying verticles
        // Deploy a verticle and don't wait for it to start
        vertx.deployVerticle("io.vertx.blog.first.OtherVerticle");

        // Deploy another instance and  want for it to start
        OtherVerticle verticle = new OtherVerticle();
        vertx.deployVerticle(verticle, res -> {
            if (res.succeeded()) {
                String deploymentID = res.result();
                System.out.println("Other verticle deployed ok, deploymentID = " + deploymentID);
                // You can also explicitly undeploy a verticle deployment.
                // Note that this is usually unnecessary as any verticles deployed by a verticle will be automatically
                // undeployed when the parent verticle is undeployed

                vertx.undeploy(deploymentID, res2 -> {
                    if (res2.succeeded()) {

                        System.out.println("Undeployed ok!");

                    } else {
                        res2.cause().printStackTrace();
                    }
                });
            } else {
                res.cause().printStackTrace();
            }
        });
    }
}

