package pro.surpin.data.arrowflight.server;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The opt-in filter selectivity estimate ({@code statistics.filterSelectivity}) in Spark local mode against the
 * in-process server, whose {@code arrowflight.table_statistics} action supplies footer bounds: the row count
 * Spark sees for the scan follows the pushed filter's share of the value range.
 */
@Tag("integration")
@Tag("spark")
class SparkFilterStatisticsTest {

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
                .appName("SparkFilterStatisticsTest")
                .master("local[2]")
                .config("spark.sql.shuffle.partitions", "4")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .config("spark.driver.host", "127.0.0.1")
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

    private static Dataset<Row> table(boolean estimate) {
        return spark.read().format("flight")
                .option("host", "localhost")
                .option("port", String.valueOf(helper.location.getUri().getPort()))
                .option("user", "test").option("password", "test")
                .option("table", "test_schema.test_table")
                .option("statistics.filterSelectivity", String.valueOf(estimate))
                .load();
    }

    /** Rows Spark estimates for a query whose filter is fully pushed, so its optimized plan is the Flight scan. */
    private static long scanRows(Dataset<Row> df) {
        LogicalPlan plan = df.queryExecution().optimizedPlan();
        assertTrue(plan.stats().rowCount().isDefined(), "scan reports a row count: " + plan);
        return plan.stats().rowCount().get().longValue();
    }

    @Test
    void rangeFilterFollowsFooterBounds() {
        Row bounds = table(false).selectExpr("min(id)", "max(id)", "count(*)").first();
        long min = ((Number) bounds.get(0)).longValue();
        long max = ((Number) bounds.get(1)).longValue();
        long rows = bounds.getLong(2);
        assertEquals(1000L, rows);

        // Above the largest value: no row can match, the scan is estimated at the one-row floor.
        Dataset<Row> none = table(true).where("id > " + max);
        assertEquals(1L, scanRows(none));
        assertEquals(0L, none.count());

        // Half of the value range: about half of the rows (the fixed default would be a third).
        long middle = min + (max - min) / 2;
        long half = scanRows(table(true).where("id <= " + middle));
        assertTrue(Math.abs(half - rows / 2) <= rows / 20, "about half of " + rows + ": " + half);

        // Columns without numeric bounds keep the textbook default.
        assertEquals(rows / 10, scanRows(table(true).where("string_col = 'x'")));
    }

    @Test
    void withoutTheOptionTheScanKeepsTheTableEstimate() {
        assertEquals(1000L, scanRows(table(false).where("id > 1000000")));
    }
}
