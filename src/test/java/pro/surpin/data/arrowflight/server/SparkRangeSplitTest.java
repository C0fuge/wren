package pro.surpin.data.arrowflight.server;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Range splits ({@code partition.byColumn} with bounds) in Spark local mode against the in-process server: every row
 * is read exactly once, also rows outside the bounds and rows with a null key.
 */
@Tag("integration")
@Tag("spark")
class SparkRangeSplitTest {

    private static final int ROWS = 120;

    @TempDir
    static Path dataDir;

    private static TestFlightServerHelper helper;
    private static SparkSession spark;

    /**
     * Writes {@code s.split} (id 0..119; {@code k = id - 5}, null for every tenth id; {@code x = k}; {@code day} =
     * 2020-01-01 plus {@code k} days) and starts Spark and the server on it.
     *
     * @throws Exception when either service cannot start
     */
    @BeforeAll
    static void startServerAndSpark() throws Exception {
        injectSparkUser(System.getProperty("user.name", "root"));
        spark = SparkSession.builder()
                .appName("SparkRangeSplitTest")
                .master("local[2]")
                .config("spark.sql.shuffle.partitions", "4")
                .config("spark.driver.bindAddress", "127.0.0.1")
                // Advertise the address bound: Spark 4 executors fetch session classes from the driver.
                .config("spark.driver.host", "127.0.0.1")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        spark.range(ROWS)
                .select(functions.col("id"),
                        functions.when(functions.col("id").mod(10).equalTo(0), functions.lit(null))
                                .otherwise(functions.col("id").minus(5)).cast("bigint").alias("k"))
                .withColumn("x", functions.col("k").cast("double"))
                .withColumn("day", functions.expr("date_add(date'2020-01-01', cast(k as int))"))
                .coalesce(1)
                .write()
                .parquet(dataDir.resolve("s/split").toString());
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

    private static Dataset<Row> split(String column, int size, String lower, String upper) {
        return spark.read()
                .format("flight")
                .option("host", "localhost")
                .option("port", String.valueOf(helper.location.getUri().getPort()))
                .option("user", "test")
                .option("password", "test")
                .option("table", "s.split")
                .option("partition.byColumn", column)
                .option("partition.size", String.valueOf(size))
                .option("partition.lowerBound", lower)
                .option("partition.upperBound", upper)
                .load();
    }

    private static void assertEveryRowOnce(Dataset<Row> df, int partitions) {
        assertEquals(partitions, df.rdd().getNumPartitions());
        List<Long> ids = df.select("id").collectAsList().stream().map(r -> r.getLong(0)).sorted()
                .collect(Collectors.toList());
        assertEquals(LongStream.range(0, ROWS).boxed().collect(Collectors.toList()), ids);
    }

    /** Bounds inside the key range: the outer slices take the rest and the null keys. */
    @Test
    void longBoundsInsideTheKeyRange() {
        assertEveryRowOnce(split("k", 4, "0", "100"), 4);
    }

    /** Bounds narrower than the slice count used to repeat the same slice. */
    @Test
    void narrowLongBounds() {
        assertEveryRowOnce(split("k", 6, "0", "2"), 2);
    }

    @Test
    void doubleBounds() {
        assertEveryRowOnce(split("x", 3, "10", "40"), 3);
    }

    @Test
    void dateBounds() {
        assertEveryRowOnce(split("day", 2, "2020-01-10", "2020-02-10"), 2);
    }

    /** A pushed filter is combined with every slice. */
    @Test
    void filterAndSplit() {
        Dataset<Row> df = split("k", 4, "0", "100").filter("id >= 100");
        assertEquals(20L, df.count());
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
