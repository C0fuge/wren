package pro.surpin.data.arrowflight.server;

import pro.surpin.data.arrowflight.client.spark.FlightAqeGuard;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runtime filtering in Spark local mode against the in-process server: with
 * {@code spark.flight.retainPushedFilters=true} the filtered side of a join keeps its filter in Spark's plan, Spark's
 * dynamic pruning collects its join keys and the other Flight scan is planned again with them; the join result is
 * the same as without. The self-join scans one table twice, so {@link FlightAqeGuard} switches AQE off for it.
 */
@Tag("integration")
@Tag("spark")
class SparkRuntimeFilterTest {

    private static TestFlightServerHelper helper;
    private static SparkSession spark;

    @BeforeAll
    static void startServerAndSpark() throws Exception {
        SparkFlightCatalogTest.injectSparkUser(System.getProperty("user.name", "root"));
        helper = TestFlightServerHelper.builder()
                .dataDir(java.nio.file.Paths.get("src/test/resources/test_db")
                        .toAbsolutePath().normalize().toString())
                .start();
        spark = SparkSession.builder()
                .appName("SparkRuntimeFilterTest")
                .master("local[2]")
                .config("spark.sql.shuffle.partitions", "4")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .config("spark.driver.host", "127.0.0.1")
                .config("spark.sql.extensions", FlightAqeGuard.class.getName())
                .config("spark.flight.runtimeFilterMinRows", "1")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
    }

    @AfterAll
    static void stopAll() throws Exception {
        if (spark != null) {
            spark.stop();
        }
        if (helper != null) {
            helper.close();
        }
    }

    @AfterEach
    void resetSettings() {
        spark.conf().unset("spark.flight.retainPushedFilters");
        spark.conf().unset("spark.flight.runtimeFilterRangeMinKeys");
        spark.conf().unset("spark.flight.retainMaxRows");
    }

    private static Dataset<Row> table() {
        return source().option("table", "test_schema.test_table").load();
    }

    private static org.apache.spark.sql.DataFrameReader source() {
        return spark.read().format("flight")
                .option("host", "localhost")
                .option("port", String.valueOf(helper.location.getUri().getPort()))
                .option("user", "test").option("password", "test")
                .option("statistics.filterSelectivity", "true");
    }

    /** Small filtered side joined with the whole table on id; returns the sorted ids and the executed plan. */
    private static Result join() {
        return join(table());
    }

    private static Result join(Dataset<Row> bigSource) {
        Dataset<Row> small = table().where("tinyint_col = 3").selectExpr("id AS sid");
        Dataset<Row> big = bigSource.selectExpr("id", "string_col");
        Dataset<Row> joined = big.join(small, big.col("id").equalTo(small.col("sid"))).select("id", "string_col")
                .orderBy("id");
        List<Row> rows = joined.collectAsList();
        // Query listeners (FlightAqeGuard restores AQE in one) run asynchronously on Spark's listener bus.
        try {
            spark.sparkContext().listenerBus().waitUntilEmpty();
        } catch (Exception e) { // TimeoutException, declared through Scala's @throws
            throw new IllegalStateException("Spark's listener bus did not drain", e);
        }
        return new Result(rows, joined.queryExecution().executedPlan().toString().toLowerCase(java.util.Locale.ROOT));
    }

    private record Result(List<Row> rows, String plan) {
    }

    @Test
    void retainedFilterPrunesTheOtherScanAtRunTime() {
        Result plain = join();
        assertFalse(plain.plan().contains("id in ("), "no runtime filter by default: " + plain.plan());
        assertEquals(100, plain.rows().size());

        spark.conf().set("spark.flight.retainPushedFilters", "true");
        Result pruned = join();
        assertTrue(pruned.plan().contains("dynamicpruningexpression(id"), "dynamic pruning planned: " + pruned.plan());
        assertTrue(pruned.plan().contains("id in ("), "big scan re-planned with the keys: " + pruned.plan());
        assertEquals(plain.rows(), pruned.rows());
        // The guard restores AQE once the self-join has ended.
        assertEquals("true", spark.conf().get("spark.sql.adaptive.enabled", "true"));
    }

    @Test
    void manyKeysBecomeARange() {
        spark.conf().set("spark.flight.retainPushedFilters", "true");
        spark.conf().set("spark.flight.runtimeFilterRangeMinKeys", "2");
        Result pruned = join();
        assertFalse(pruned.plan().contains("id in ("), pruned.plan());
        assertTrue(pruned.plan().contains(">="), "big scan re-planned with the key range: " + pruned.plan());
        assertEquals(100, pruned.rows().size());
    }

    @Test
    void prunesWithAdaptiveExecutionWhenNoTableIsScannedTwice() {
        spark.conf().set("spark.flight.retainPushedFilters", "true");
        assertEquals("true", spark.conf().get("spark.sql.adaptive.enabled", "true"), "AQE on before");
        Result pruned = join(source().option("table", "select id, string_col from test_schema.test_table").load());
        assertTrue(pruned.plan().contains("adaptivesparkplan"), "AQE stays on: " + pruned.plan());
        assertTrue(pruned.plan().contains("id in ("), "query scan re-planned with the keys: " + pruned.plan());
        assertEquals(join().rows(), pruned.rows());
    }

    @Test
    void autoModeRetainsOnlySmallSelectiveTables() {
        spark.conf().set("spark.flight.retainPushedFilters", "auto");
        // 1000 rows, equality on a column without distinct counts: estimated 0.1 of the rows, within the defaults.
        Result pruned = join();
        assertTrue(pruned.plan().contains("id in ("), "auto keeps the small filtered table's filter: " + pruned.plan());
        assertEquals(100, pruned.rows().size());

        spark.conf().set("spark.flight.retainMaxRows", "999");
        Result plain = join();
        assertFalse(plain.plan().contains("id in ("), "table above retainMaxRows: " + plain.plan());
        assertEquals(pruned.rows(), plain.rows());
    }

    @Test
    void offeredToSparksPruningOnlyWhenEnabled() {
        // A filtered non-Flight side: Spark sees its filter in any case, so only the setting decides.
        java.util.function.Supplier<Result> join = () -> {
            Dataset<Row> keys = spark.range(0, 1000).where("id % 97 = 5").selectExpr("CAST(id AS INT) AS sid");
            // Whole-file bytes: with the narrower two-column estimate the test table is too small for Spark to
            // expect a benefit from pruning it with the range's keys.
            Dataset<Row> big = source().option("table", "test_schema.test_table")
                    .option("statistics.columnWidth", "false").load().selectExpr("id", "string_col");
            Dataset<Row> joined = big.join(keys, big.col("id").equalTo(keys.col("sid"))).select("id").orderBy("id");
            List<Row> rows = joined.collectAsList();
            String plan = joined.queryExecution().executedPlan().toString();
            return new Result(rows, plan.toLowerCase(java.util.Locale.ROOT));
        };
        Result plain = join.get();
        assertFalse(plain.plan().contains("id in ("), "nothing offered by default: " + plain.plan());
        assertEquals(11, plain.rows().size());

        spark.conf().set("spark.flight.retainPushedFilters", "true");
        Result pruned = join.get();
        assertTrue(pruned.plan().contains("id in (5,102,"), "pruned with the other side's keys: " + pruned.plan());
        assertEquals(plain.rows(), pruned.rows());
    }
}
