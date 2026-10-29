package pro.surpin.data.arrowflight.server;

import pro.surpin.data.arrowflight.client.spark.FlightPushedCounts;
import pro.surpin.data.arrowflight.client.spark.FlightSessionCatalog;
import pro.surpin.data.arrowflight.client.spark.FlightSource;
import pro.surpin.data.arrowflight.client.spark.read.FlightScan;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies full TPC-H Q1 aggregation pushdown through the Spark Flight connector.
 */
@Tag("integration")
@Tag("spark")
@Tag("smoke")
class SparkTpchQ1PushdownIntegrationTest {

    private static final String Q1 = """
            SELECT
              l_returnflag,
              l_linestatus,
              SUM(l_quantity) AS sum_qty,
              SUM(l_extendedprice) AS sum_base_price,
              SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
              SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
              AVG(l_quantity) AS avg_qty,
              AVG(l_extendedprice) AS avg_price,
              AVG(l_discount) AS avg_disc,
              COUNT(*) AS count_order
            FROM %s
            WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
            GROUP BY l_returnflag, l_linestatus
            ORDER BY l_returnflag, l_linestatus
            """;

    private static TestFlightServerHelper helper;
    private static SparkSession spark;

    /**
     * Starts an embedded Flight server and ANSI-enabled local Spark session.
     *
     * @throws Exception when either service cannot start
     */
    @BeforeAll
    static void startServerAndSpark() throws Exception {
        injectSparkUser(System.getProperty("user.name", "root"));
        Path dataDir = Paths.get("src/test/resources/lineitem_tiny")
                .toAbsolutePath().normalize();
        helper = TestFlightServerHelper.builder()
                .dataDir(dataDir.toString())
                .start();

        spark = SparkSession.builder()
                .appName("SparkTpchQ1PushdownIntegrationTest")
                .master("local[2]")
                .config("spark.sql.ansi.enabled", "true")
                .config("spark.sql.shuffle.partitions", "4")
                .config("spark.sql.catalog.spark_catalog", FlightSessionCatalog.class.getName())
                .config("spark.driver.bindAddress", "127.0.0.1")
                // Advertise the address bound: Spark 4 executors fetch session classes from the driver.
                .config("spark.driver.host", "127.0.0.1")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        flightLineitem().createOrReplaceTempView("flight_lineitem");
        spark.sql("DROP TABLE IF EXISTS persisted_flight_lineitem");
        spark.sql("""
                CREATE TABLE persisted_flight_lineitem
                USING %s
                OPTIONS (
                  host 'localhost',
                  port '%d',
                  user 'test',
                  password 'test',
                  table 'tpch.lineitem'
                )
                """.formatted(FlightSource.class.getName(), helper.location.getUri().getPort()));
        spark.read()
                .parquet(dataDir.resolve("tpch/lineitem").toString())
                .createOrReplaceTempView("direct_lineitem");
    }

    /**
     * Stops Spark and the embedded Flight server.
     *
     * @throws Exception when resource cleanup fails
     */
    @AfterAll
    static void stopAll() throws Exception {
        if (spark != null) {
            spark.sql("DROP TABLE IF EXISTS persisted_flight_lineitem");
            spark.stop();
        }
        if (helper != null) {
            helper.close();
        }
    }

    /**
     * Checks that Spark pushes Q1 partial aggregates and preserves direct Parquet results.
     */
    @Test
    void fullQ1PushesAggregatesAndMatchesDirectParquet() {
        assertQ1PushdownAndResults("flight_lineitem");
    }

    /**
     * Checks that a persisted Flight table retains Q1 aggregate pushdown.
     */
    @Test
    void persistedQ1PushesAggregatesAndMatchesDirectParquet() {
        assertQ1PushdownAndResults("persisted_flight_lineitem");
    }

    /**
     * Without ANSI mode (Spark 3.5's default) Spark translates no {@code + - * /} into connector expressions, so
     * Q1's arithmetic aggregates keep the whole aggregation in Spark (same results); aggregates of plain columns
     * are still pushed. Documents why TPC-H Q1 runs in Spark unless {@code spark.sql.ansi.enabled=true}.
     */
    @Test
    void nonAnsiPushesOnlyAggregatesWithoutArithmetic() {
        spark.conf().set("spark.sql.ansi.enabled", "false");
        try {
            Dataset<Row> flightResult = q1("flight_lineitem");
            String plan = flightResult.queryExecution().executedPlan().toString().toLowerCase();
            assertFalse(plan.contains("group by"),
                    "Spark is expected to keep Q1 without ANSI mode (arithmetic not translated):\n" + plan);
            assertEquals(q1("direct_lineitem").collectAsList(), flightResult.collectAsList(),
                    "Flight and direct Q1 results must match without ANSI mode");

            String plain = """
                    SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty, COUNT(*) AS count_order
                    FROM %s GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus
                    """;
            Dataset<Row> pushed = spark.sql(plain.formatted("flight_lineitem"));
            String pushedPlan = pushed.queryExecution().executedPlan().toString().toLowerCase();
            assertTrue(pushedPlan.contains("group by") && pushedPlan.contains("sum("),
                    "Aggregates of plain columns must be pushed without ANSI mode:\n" + pushedPlan);
            assertEquals(spark.sql(plain.formatted("direct_lineitem")).collectAsList(), pushed.collectAsList(),
                    "Flight and direct results must match");
        } finally {
            spark.conf().set("spark.sql.ansi.enabled", "true");
        }
    }

    /**
     * TPC-H Q17 shape under ANSI mode: the correlated subquery's {@code avg} is pushed as partial SUM + COUNT. Spark
     * 3.5 evaluates the subquery's aggregates on zero rows while planning, where its merge of the pushed counts
     * ({@code sum}) is null; without {@link FlightPushedCounts} that divided null by null and failed with
     * {@code [ARITHMETIC_OVERFLOW] Overflow in sum of decimals}. Results must equal the direct Parquet read, also for
     * outer rows whose subquery has no rows (later ship dates filtered out in the subquery). Spark 4.0 does not push
     * this subquery's avg; there only the results are compared.
     */
    @Test
    void correlatedAvgSubqueryUnderAnsiMatchesDirectParquet() {
        String q17 = """
                SELECT count(*) AS n, sum(l_extendedprice) AS total FROM direct_lineitem o
                WHERE l_quantity <= (SELECT avg(l_quantity) FROM %s i
                                    WHERE i.l_partkey = o.l_partkey%s)
                """;
        for (String extra : List.of("", " AND i.l_shipdate < DATE '1998-07-01'")) {
            Dataset<Row> flight = spark.sql(q17.formatted("flight_lineitem", extra));
            String plan = flight.queryExecution().executedPlan().toString().toLowerCase();
            boolean pushed = plan.contains("group by") && plan.contains("count(");
            // Spark 3.5 pushes the subquery's avg (the failing case); Spark 4.0 keeps it in Spark.
            assertTrue(pushed || !org.apache.spark.package$.MODULE$.SPARK_VERSION().startsWith("3."),
                    "The subquery's avg must be pushed as partial aggregates:\n" + plan);
            String optimized = flight.queryExecution().optimizedPlan().toString().toLowerCase();
            assertTrue(!pushed || optimized.contains("coalesce(sum(agg_func_"),
                    "The merged counts must be wrapped:\n" + optimized);
            Row expected = spark.sql(q17.formatted("direct_lineitem", extra)).collectAsList().get(0);
            assertTrue(expected.getLong(0) > 0, "The query must select rows");
            assertEquals(expected, flight.collectAsList().get(0), "Flight and direct results must match");
        }
    }

    /**
     * The rule wraps each merged count once: applying it again leaves the plan unchanged, and Q1's
     * {@code count(*)} becomes non-null as Spark's own count.
     */
    @Test
    void pushedCountsRuleIsIdempotent() {
        Dataset<Row> flight = q1("flight_lineitem");
        LogicalPlan optimized = flight.queryExecution().optimizedPlan();
        assertTrue(optimized.toString().toLowerCase().contains("coalesce("), "Q1's counts must be wrapped:\n"
                + optimized);
        assertTrue(FlightPushedCounts.instance().apply(optimized).fastEquals(optimized),
                "A second application must not change the plan");
        assertFalse(flight.schema().apply("count_order").nullable(), "count(*) must stay non-null");
    }

    /**
     * TPC-H Q13 shape: a {@code [NOT] LIKE} with two wildcards inside is no connector predicate in Spark, so
     * {@code FlightLikePushdown} moves it into the scan's WHERE clause after the push-down. Results equal the direct
     * Parquet read; a pattern with {@code _} and the setting {@code false} keep the filter in Spark only.
     */
    @Test
    void multiWildcardLikeIsSentToTheServerAndMatchesDirectParquet() {
        String query = "SELECT l_orderkey, l_linenumber, l_comment FROM %s WHERE l_comment %s"
                + " ORDER BY l_orderkey, l_linenumber";
        // l_comment is "Debug row <n>"; the lower-case pattern checks that the server matches case-sensitively.
        Map<String, String> sent = Map.of(
                "NOT LIKE '%b%w 0%'", "like '%b%w 0%' escape",
                "LIKE 'D%row 9%'", "like 'd%row 9%' escape",
                "NOT LIKE 'debug%row%'", "like 'debug%row%' escape",
                "LIKE '%e%g%' AND l_comment NOT LIKE '%w 1%'", "like '%e%g%' escape");
        for (Map.Entry<String, String> entry : sent.entrySet()) {
            String condition = entry.getKey();
            Dataset<Row> flight = spark.sql(query.formatted("flight_lineitem", condition));
            String scan = flightScanDescription(flight);
            assertTrue(scan.contains(entry.getValue()), "The LIKE must reach the server:\n" + scan);
            List<Row> expected = spark.sql(query.formatted("direct_lineitem", condition)).collectAsList();
            assertFalse(expected.isEmpty(), "The query must select rows: " + condition);
            assertEquals(expected, flight.collectAsList(), "Flight and direct results must match: " + condition);
        }

        Dataset<Row> underscore = spark.sql(query.formatted("flight_lineitem", "LIKE '%row _9%'"));
        assertFalse(flightScanDescription(underscore).contains(" like "), "A '_' pattern must stay in Spark");
        assertEquals(spark.sql(query.formatted("direct_lineitem", "LIKE '%row _9%'")).collectAsList(),
                underscore.collectAsList(), "Flight and direct results must match with '_'");

        // As in Q13: the column only the LIKE needs is no longer read, and Spark has no LIKE left to evaluate.
        String join = "SELECT c.l_orderkey, count(o.l_linenumber) AS n FROM direct_lineitem c LEFT OUTER JOIN %s o"
                + " ON c.l_orderkey = o.l_orderkey AND o.l_comment NOT LIKE '%%b%%w 0%%'"
                + " GROUP BY c.l_orderkey ORDER BY c.l_orderkey";
        Dataset<Row> pruned = spark.sql(join.formatted("flight_lineitem"));
        assertTrue(flightScanDescription(pruned).contains("like '%b%w 0%' escape"), "The LIKE must be pushed");
        String optimized = pruned.queryExecution().optimizedPlan().toString().toLowerCase();
        List<String> sparkFilters = new java.util.ArrayList<>();
        pruned.queryExecution().optimizedPlan().foreach(new scala.runtime.AbstractFunction1<LogicalPlan,
                scala.runtime.BoxedUnit>() {
            @Override
            public scala.runtime.BoxedUnit apply(LogicalPlan node) {
                if (node instanceof org.apache.spark.sql.catalyst.plans.logical.Filter filter) {
                    sparkFilters.add(filter.condition().sql().toLowerCase());
                }
                return scala.runtime.BoxedUnit.UNIT;
            }
        });
        assertTrue(sparkFilters.stream().noneMatch(f -> f.contains(" like ")),
                "Spark must not evaluate the pushed LIKE:\n" + optimized);
        assertFalse(flightScanColumns(pruned).contains("l_comment"), "l_comment must not be read:\n" + optimized);
        assertEquals(spark.sql(join.formatted("direct_lineitem")).collectAsList(), pruned.collectAsList(),
                "Flight and direct join results must match");

        spark.conf().set("spark.flight.likePushdown", "false");
        try {
            Dataset<Row> off = spark.sql(query.formatted("flight_lineitem", "NOT LIKE '%b%w 0%'"));
            assertFalse(flightScanDescription(off).contains(" like "), "The setting must keep LIKE in Spark");
            assertTrue(flightScanColumns(spark.sql(join.formatted("flight_lineitem"))).contains("l_comment"),
                    "Without the rule Spark reads l_comment for its own filter");
        } finally {
            spark.conf().unset("spark.flight.likePushdown");
        }
    }

    /**
     * Returns the columns of the only Flight scan of a query.
     *
     * @param query the query
     * @return the column names
     */
    private static List<String> flightScanColumns(Dataset<Row> query) {
        List<String> columns = new java.util.ArrayList<>();
        query.queryExecution().optimizedPlan().foreach(new scala.runtime.AbstractFunction1<LogicalPlan,
                scala.runtime.BoxedUnit>() {
            @Override
            public scala.runtime.BoxedUnit apply(LogicalPlan node) {
                if (node instanceof DataSourceV2ScanRelation relation && relation.scan() instanceof FlightScan) {
                    columns.addAll(java.util.Arrays.asList(relation.schema().fieldNames()));
                    columns.addAll(java.util.Arrays.asList(relation.scan().readSchema().fieldNames()));
                }
                return scala.runtime.BoxedUnit.UNIT;
            }
        });
        return columns;
    }

    /**
     * Returns the server statement of the only Flight scan of a query (after the optimizer rules ran).
     *
     * @param query the query
     * @return the lower-case scan description
     */
    private static String flightScanDescription(Dataset<Row> query) {
        List<String> scans = new java.util.ArrayList<>();
        query.queryExecution().optimizedPlan().foreach(new scala.runtime.AbstractFunction1<LogicalPlan,
                scala.runtime.BoxedUnit>() {
            @Override
            public scala.runtime.BoxedUnit apply(LogicalPlan node) {
                if (node instanceof DataSourceV2ScanRelation relation
                        && relation.scan() instanceof FlightScan flightScan) {
                    scans.add(flightScan.description().toLowerCase());
                }
                return scala.runtime.BoxedUnit.UNIT;
            }
        });
        assertEquals(1, scans.size(), "Expected one Flight scan");
        return scans.get(0);
    }

    /**
     * Checks Q1 aggregate pushdown and results for one Flight-backed relation.
     *
     * @param table Flight-backed table or view name
     */
    private static void assertQ1PushdownAndResults(String table) {
        Dataset<Row> flightResult = q1(table);
        Dataset<Row> directResult = q1("direct_lineitem");

        String plan = flightResult.queryExecution().executedPlan().toString().toLowerCase();
        assertTrue(plan.contains("group by"),
                "Q1 must push GROUP BY into the Flight scan:\n" + plan);
        assertTrue(plan.contains("sum("),
                "Q1 must push decimal SUM expressions into the Flight scan:\n" + plan);
        assertTrue(plan.contains("count("),
                "Q1 must push COUNT partials for AVG into the Flight scan:\n" + plan);

        List<Row> expected = directResult.collectAsList();
        List<Row> actual = flightResult.collectAsList();
        assertEquals(directResult.schema(), flightResult.schema(),
                "Flight and direct Q1 schemas must match");
        assertEquals(expected, actual, "Flight and direct Q1 results must match");
    }

    /**
     * Loads the tiny lineitem table through the Flight DataSource.
     *
     * @return Flight-backed lineitem dataset
     */
    private static Dataset<Row> flightLineitem() {
        return spark.read()
                .format("flight")
                .option("host", "localhost")
                .option("port", String.valueOf(helper.location.getUri().getPort()))
                .option("user", "test")
                .option("password", "test")
                .option("table", "tpch.lineitem")
                .load();
    }

    /**
     * Builds the benchmark Q1 query for one registered lineitem view.
     *
     * @param table Spark view name
     * @return Q1 result dataset
     */
    private static Dataset<Row> q1(String table) {
        return spark.sql(Q1.formatted(table));
    }

    /**
     * Injects the Spark user required by Hadoop on Java 21.
     *
     * @param user Spark user name
     */
    @SuppressWarnings("unchecked")
    private static void injectSparkUser(String user) {
        try {
            Class<?> processEnvironment = Class.forName("java.lang.ProcessEnvironment");
            Field unmodifiableField =
                    processEnvironment.getDeclaredField("theUnmodifiableEnvironment");
            unmodifiableField.setAccessible(true);
            Map<String, String> unmodifiableEnvironment =
                    (Map<String, String>) unmodifiableField.get(null);
            Field mutableField = unmodifiableEnvironment.getClass().getDeclaredField("m");
            mutableField.setAccessible(true);
            Map<String, String> environment =
                    (Map<String, String>) mutableField.get(unmodifiableEnvironment);
            environment.put("SPARK_USER", user);
        } catch (ReflectiveOperationException exception) {
            System.err.println("Warning: could not inject SPARK_USER (" + exception + ")");
        }
    }
}
