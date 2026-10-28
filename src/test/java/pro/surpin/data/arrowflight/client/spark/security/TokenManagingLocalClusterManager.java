package pro.surpin.data.arrowflight.client.spark.security;

import org.apache.spark.SparkContext;
import org.apache.spark.deploy.LocalSparkCluster;
import org.apache.spark.deploy.security.HadoopDelegationTokenManager;
import org.apache.spark.scheduler.ExternalClusterManager;
import org.apache.spark.scheduler.SchedulerBackend;
import org.apache.spark.scheduler.TaskScheduler;
import org.apache.spark.scheduler.TaskSchedulerImpl;
import org.apache.spark.scheduler.cluster.StandaloneSchedulerBackend;

import scala.Option;
import scala.runtime.BoxedUnit;

/**
 * Spark master {@value #MASTER}: a {@code local-cluster[1,1,1024]} (one worker, executors in their
 * own JVMs) whose scheduler backend runs Spark's delegation token manager as the YARN backend does.
 * Spark's standalone backend never creates one, so plain {@code local-cluster} cannot show that the
 * token manager ships Flight sessions to executors. Registered in
 * {@code META-INF/services/org.apache.spark.scheduler.ExternalClusterManager} (test class path only).
 */
public final class TokenManagingLocalClusterManager implements ExternalClusterManager {
    /** Master URL served by this manager. */
    public static final String MASTER = "flight-token-local-cluster";

    @Override
    public boolean canCreate(String masterUrl) {
        return MASTER.equals(masterUrl);
    }

    @Override
    public TaskScheduler createTaskScheduler(SparkContext sc, String masterUrl) {
        return new TaskSchedulerImpl(sc);
    }

    @Override
    public SchedulerBackend createSchedulerBackend(SparkContext sc, String masterUrl, TaskScheduler scheduler) {
        LocalSparkCluster cluster = LocalSparkCluster.apply(1, 1, 1024, sc.conf());
        String[] masters = cluster.start();
        StandaloneSchedulerBackend backend = new StandaloneSchedulerBackend((TaskSchedulerImpl) scheduler, sc,
                masters) {
            @Override
            public Option<HadoopDelegationTokenManager> createTokenManager() {
                return Option.apply(new HadoopDelegationTokenManager(sc.conf(), sc.hadoopConfiguration(),
                        driverEndpoint()));
            }
        };
        backend.shutdownCallback_$eq(stopped -> {
            cluster.stop();
            return BoxedUnit.UNIT;
        });
        return backend;
    }

    @Override
    public void initialize(TaskScheduler scheduler, SchedulerBackend backend) {
        ((TaskSchedulerImpl) scheduler).initialize(backend);
    }
}
