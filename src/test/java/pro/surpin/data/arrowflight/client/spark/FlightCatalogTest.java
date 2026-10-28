package pro.surpin.data.arrowflight.client.spark;

import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests of {@link FlightCatalog} parts that need no server.
 */
@Tag("unit")
class FlightCatalogTest {

    private static FlightCatalog catalog(Map<String, String> options) {
        FlightCatalog catalog = new FlightCatalog();
        catalog.initialize("flight", new CaseInsensitiveStringMap(options));
        return catalog;
    }

    private static Map<String, String> connection() {
        return Map.of("host", "localhost", "port", "1", "user", "u", "password", "p");
    }

    @Test
    void plainNamesStayUnquoted() {
        assertEquals("test_schema.test_table", FlightCatalog.qualifiedName("test_schema", "test_table"));
    }

    @Test
    void otherNamesAreQuoted() {
        assertEquals("\"my-schema\".\"a \"\"b\"\"\"", FlightCatalog.qualifiedName("my-schema", "a \"b\""));
        assertEquals("\"1x\"", FlightCatalog.quoteIfNeeded("1x"));
    }

    @Test
    void scanDefaultsDropTableAndLowerCaseKeys() {
        Map<String, String> defaults = FlightCatalog.scanDefaults(new CaseInsensitiveStringMap(
                Map.of("Partition.Size", "3", "TABLE", "s.t", "host", "h")));
        assertEquals(Map.of("partition.size", "3", "host", "h"), defaults);
    }

    @Test
    void scanOptionsOverrideDefaults() {
        CaseInsensitiveStringMap merged = FlightTable.withDefaults(
                Map.of("partition.size", "3", "statistics.filterselectivity", "true"),
                new CaseInsensitiveStringMap(Map.of("PARTITION.SIZE", "8")));
        assertEquals("8", merged.get("partition.size"));
        assertEquals("true", merged.get("statistics.filterSelectivity"));
    }

    @Test
    void noDefaultsKeepsScanOptions() {
        CaseInsensitiveStringMap options = new CaseInsensitiveStringMap(Map.of("a", "b"));
        assertSame(options, FlightTable.withDefaults(Map.of(), options));
    }

    @Test
    void defaultNamespaceFollowsDefaultSchema() {
        assertArrayEquals(new String[0], catalog(connection()).defaultNamespace());
        Map<String, String> options = new java.util.HashMap<>(connection());
        options.put("default.schema", "tpch");
        FlightCatalog catalog = catalog(options);
        assertArrayEquals(new String[] {"tpch"}, catalog.defaultNamespace());
        assertEquals("flight", catalog.name());
    }

    @Test
    void missingCredentialsFailAtInitialize() {
        assertThrows(IllegalArgumentException.class, () -> catalog(Map.of("host", "localhost")));
    }

    @Test
    void ddlIsRejectedWithoutServerCall() {
        FlightCatalog catalog = catalog(connection());
        Identifier ident = Identifier.of(new String[] {"s"}, "t");
        assertThrows(UnsupportedOperationException.class, () -> catalog.dropTable(ident));
        assertThrows(UnsupportedOperationException.class, () -> catalog.renameTable(ident, ident));
        assertThrows(UnsupportedOperationException.class, () -> catalog.alterTable(ident));
        assertThrows(UnsupportedOperationException.class,
                () -> catalog.createTable(ident, new org.apache.spark.sql.types.StructType(),
                        new org.apache.spark.sql.connector.expressions.Transform[0], Map.of()));
        assertThrows(UnsupportedOperationException.class, () -> catalog.createNamespace(new String[] {"s"}, Map.of()));
        assertThrows(UnsupportedOperationException.class, () -> catalog.alterNamespace(new String[] {"s"}));
        assertThrows(UnsupportedOperationException.class, () -> catalog.dropNamespace(new String[] {"s"}, true));
    }

    @Test
    void multiLevelIdentifiersAreNoTables() {
        FlightCatalog catalog = catalog(connection());
        assertThrows(org.apache.spark.sql.catalyst.analysis.NoSuchTableException.class,
                () -> catalog.loadTable(Identifier.of(new String[] {"a", "b"}, "t")));
        assertFalse(catalog.tableExists(Identifier.of(new String[0], "t")));
        assertFalse(catalog.namespaceExists(new String[] {"a", "b"}));
    }
}
