package pro.surpin.data.arrowflight.server;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A write the server rejects fails the Spark job with the server's message and its Flight cause chain: Arrow's
 * {@code FlightRuntimeException} is not serializable, so Spark used to report the executor's failure as a message
 * only (or, without spark-submit's {@code --add-opens}, the task thread died and the job hung).
 */
@Tag("integration")
@Tag("spark")
class SparkFlightWriteErrorTest {

    @TempDir
    static Path dataDir;

    private static TestFlightServerHelper helper;
    private static SparkSession spark;

    /**
     * Writes a small Parquet table {@code s.t} and starts Spark and the server on it.
     *
     * @throws Exception when either service cannot start
     */
    @BeforeAll
    static void startServerAndSpark() throws Exception {
        injectSparkUser(System.getProperty("user.name", "root"));
        spark = SparkSession.builder()
                .appName("SparkFlightWriteErrorTest")
                .master("local[2]")
                .config("spark.sql.shuffle.partitions", "2")
                .config("spark.driver.bindAddress", "127.0.0.1")
                // Advertise the address bound: Spark 4 executors fetch session classes from the driver.
                .config("spark.driver.host", "127.0.0.1")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        spark.range(10).selectExpr("id", "cast(id as double) as x").coalesce(1)
                .write().parquet(dataDir.resolve("s/t").toString());
        helper = TestFlightServerHelper.builder().dataDir(dataDir.toString()).start();
    }

    /**
     * Stops Spark and the server.
     *
     * @throws Exception when cleanup fails
     */
    @AfterAll
    static void stopAll() throws Exception {
        if (spark != null) {
            spark.stop();
        }
        if (helper != null) {
            helper.close();
        }
    }

    /** The in-process server serves Parquet read-only and rejects the insert on the executor. */
    @Test
    @Timeout(180)
    void rejectedWriteFailsTheJobWithTheServerError() {
        Dataset<Row> rows = spark.range(3).selectExpr("id", "cast(id as double) as x");
        Exception ex = assertThrows(Exception.class, () -> rows.write().format("flight")
                .option("host", "localhost")
                .option("port", String.valueOf(helper.location.getUri().getPort()))
                .option("user", "test")
                .option("password", "test")
                .option("table", "s.t")
                .mode("append")
                .save());
        List<String> chain = new ArrayList<>();
        for (Throwable t = ex; t != null && chain.size() < 20; t = t.getCause()) {
            chain.add(t.getClass().getName() + ": " + t.getMessage());
        }
        String all = String.join("\n", chain);
        assertTrue(all.contains("INVALID_ARGUMENT"), "the server's error must reach the driver:\n" + all);
        assertTrue(chain.stream().anyMatch(c -> c.startsWith(
                        "pro.surpin.data.arrowflight.client.spark.read.SerializableErrors$RemoteFlightError: "
                                + "org.apache.arrow.flight.FlightRuntimeException")),
                "the executor's exception (with its Flight cause) must reach the driver:\n" + all);
    }

    @SuppressWarnings("unchecked")
    private static void injectSparkUser(String user) {
        try {
            Class<?> pe = Class.forName("java.lang.ProcessEnvironment");
            Field theUnmod = pe.getDeclaredField("theUnmodifiableEnvironment");
            theUnmod.setAccessible(true);
            Map<String, String> unmodEnv = (Map<String, String>) theUnmod.get(null);
            Field m = unmodEnv.getClass().getDeclaredField("m");
            m.setAccessible(true);
            ((Map<String, String>) m.get(unmodEnv)).put("SPARK_USER", user);
        } catch (Exception ignored) {
            // best effort
        }
    }
}
