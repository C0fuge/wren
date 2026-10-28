package pro.surpin.data.arrowflight.server;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * {@code FlightCatalog} in Spark local mode against the in-process server: namespaces, tables,
 * {@code DESCRIBE} and reads (with pushdown) through {@code spark.sql.catalog.flight}.
 */
@Tag("integration")
@Tag("spark")
class SparkFlightCatalogTest {

    private static TestFlightServerHelper helper;
    private static SparkSession spark;

    @BeforeAll
    static void startServerAndSpark() throws Exception {
        injectSparkUser(System.getProperty("user.name", "root"));
        helper = TestFlightServerHelper.builder()
                .dataDir(java.nio.file.Paths.get("src/test/resources/test_db")
                        .toAbsolutePath().normalize().toString())
                .start();
        String prefix = "spark.sql.catalog.flight";
        spark = SparkSession.builder()
                .appName("SparkFlightCatalogTest")
                .master("local[2]")
                .config("spark.sql.shuffle.partitions", "4")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .config("spark.driver.host", "127.0.0.1")
                .config(prefix, "pro.surpin.data.arrowflight.client.spark.FlightCatalog")
                .config(prefix + ".host", "localhost")
                .config(prefix + ".port", String.valueOf(helper.location.getUri().getPort()))
                .config(prefix + ".user", "test")
                .config(prefix + ".password", "test")
                .config(prefix + ".partition.size", "3")
                .config(prefix + "_default", "pro.surpin.data.arrowflight.client.spark.FlightCatalog")
                .config(prefix + "_default.host", "localhost")
                .config(prefix + "_default.port", String.valueOf(helper.location.getUri().getPort()))
                .config(prefix + "_default.user", "test")
                .config(prefix + "_default.password", "test")
                .config(prefix + "_default.default.schema", "test_schema")
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

    private static List<String> column(String sql, int index) {
        return spark.sql(sql).collectAsList().stream().map(r -> r.getString(index)).collect(Collectors.toList());
    }

    @Test
    void showNamespacesListsServerSchemas() {
        assertEquals(List.of("test_schema"), column("SHOW NAMESPACES IN flight", 0));
    }

    @Test
    void showTablesListsServerTables() {
        assertEquals(List.of("test_table"), column("SHOW TABLES IN flight.test_schema", 1));
    }

    @Test
    void describeShowsServerColumns() {
        List<String> columns = column("DESCRIBE TABLE flight.test_schema.test_table", 0);
        assertTrue(columns.contains("id"), "columns: " + columns);
        assertTrue(columns.contains("string_col"), "columns: " + columns);
    }

    @Test
    void selectReadsThroughTheConnectorWithPushdown() {
        Dataset<Row> df = spark.sql("SELECT id, string_col FROM flight.test_schema.test_table WHERE id > 100");
        String plan = df.queryExecution().executedPlan().toString().toLowerCase();
        assertTrue(plan.contains("id > 100") || plan.contains("id>100"), "filter not pushed:\n" + plan);
        assertEquals(899L, df.count());
        assertEquals(1000L, spark.sql("SELECT count(*) FROM flight.test_schema.test_table").first().getLong(0));
    }

    @Test
    void catalogTableMatchesFormatFlightRead() {
        Dataset<Row> viaCatalog = spark.table("flight.test_schema.test_table");
        Dataset<Row> viaFormat = spark.read().format("flight")
                .option("host", "localhost")
                .option("port", String.valueOf(helper.location.getUri().getPort()))
                .option("user", "test").option("password", "test")
                .option("table", "test_schema.test_table").load();
        assertEquals(viaFormat.schema(), viaCatalog.schema());
        assertEquals(0L, viaCatalog.exceptAll(viaFormat).count());
        assertEquals(0L, viaFormat.exceptAll(viaCatalog).count());
    }

    @Test
    void missingTableAndNamespaceAreAnalysisErrors() {
        assertThrows(AnalysisException.class, () -> spark.sql("SELECT * FROM flight.test_schema.nope"));
        assertThrows(AnalysisException.class, () -> spark.sql("SELECT * FROM flight.nope.test_table"));
        assertThrows(AnalysisException.class, () -> spark.sql("SHOW TABLES IN flight.nope"));
        // '_' is a LIKE wildcard on the server: a near-miss name must not match.
        assertThrows(AnalysisException.class, () -> spark.sql("SELECT * FROM flight.test_schema.testXtable"));
    }

    @Test
    void useResolvesUnqualifiedNames() {
        try {
            spark.sql("USE flight_default");
            assertEquals(1000L, spark.sql("SELECT count(*) FROM test_table").first().getLong(0));
            spark.sql("USE flight.test_schema");
            assertEquals(899L, spark.sql("SELECT id FROM test_table WHERE id > 100").count());
        } finally {
            spark.sql("USE spark_catalog.default");
        }
    }

    @Test
    void ddlIsRejected() {
        assertThrows(Exception.class, () -> spark.sql("DROP TABLE flight.test_schema.test_table"));
        assertThrows(Exception.class, () -> spark.sql("CREATE NAMESPACE flight.other"));
        assertEquals(1000L, spark.table("flight.test_schema.test_table").count());
    }

    @SuppressWarnings("unchecked")
    static void injectSparkUser(String user) throws Exception {
        Class<?> pe = Class.forName("java.lang.ProcessEnvironment");
        Field theUnmod = pe.getDeclaredField("theUnmodifiableEnvironment");
        theUnmod.setAccessible(true);
        Map<String, String> unmodEnv = (Map<String, String>) theUnmod.get(null);
        Field m = unmodEnv.getClass().getDeclaredField("m");
        m.setAccessible(true);
        ((Map<String, String>) m.get(unmodEnv)).put("SPARK_USER", user);
    }
}
