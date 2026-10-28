package pro.surpin.data.arrowflight.client.spark.read;

import pro.surpin.data.arrowflight.client.Configuration;
import pro.surpin.data.arrowflight.client.model.Table;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.sources.LessThanOrEqual;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests Spark statistics reported by Flight scans.
 */
class FlightScanTest {

    /** Verifies byte and row estimates are exposed to Spark's optimizer. */
    @Test
    void reportsFlightInfoStatistics() {
        Table table = mock(Table.class);
        when(table.getEstimatedBytes()).thenReturn(2048L);
        when(table.getEstimatedRows()).thenReturn(64L);
        FlightScan scan = new FlightScan(mock(Configuration.class), table);

        Statistics statistics = scan.estimateStatistics();

        assertEquals(2048L, statistics.sizeInBytes().orElseThrow());
        assertEquals(64L, statistics.numRows().orElseThrow());
    }

    /** Verifies absent server estimates remain unknown instead of becoming zero. */
    @Test
    void keepsMissingStatisticsUnknown() {
        Table table = mock(Table.class);
        when(table.getEstimatedBytes()).thenReturn(-1L);
        when(table.getEstimatedRows()).thenReturn(-1L);
        FlightScan scan = new FlightScan(mock(Configuration.class), table);

        Statistics statistics = scan.estimateStatistics();

        assertTrue(statistics.sizeInBytes().isEmpty());
        assertTrue(statistics.numRows().isEmpty());
    }

    private static Table keyedTable(long rows) {
        Table table = mock(Table.class);
        when(table.getEstimatedRows()).thenReturn(rows);
        when(table.getSparkSchema()).thenReturn(new StructType()
                .add("id", DataTypes.LongType).add("k", DataTypes.IntegerType).add("s", DataTypes.StringType));
        return table;
    }

    /** Without a replanner (aggregated scan, default construction) no column is offered to runtime filtering. */
    @Test
    void offersNoRuntimeFilterWithoutReplanner() {
        assertEquals(0, new FlightScan(mock(Configuration.class), keyedTable(1L << 40)).filterAttributes().length);
    }

    /** Integer columns of a big table are offered; a table below the minimum row count is not. */
    @Test
    void offersIntegerColumnsOfBigTables() {
        FlightScan big = new FlightScan(mock(Configuration.class),
                keyedTable(FlightScan.DEFAULT_RUNTIME_FILTER_MIN_ROWS),
                extra -> { }, false);
        assertEquals(List.of("id", "k"), java.util.Arrays.stream(big.filterAttributes())
                .map(r -> r.fieldNames()[0]).toList());
        FlightScan small = new FlightScan(mock(Configuration.class), keyedTable(0L), extra -> { }, false);
        assertEquals(0, small.filterAttributes().length);
    }

    /** A few keys stay an IN list, many integer keys become their min/max range, too many others are dropped. */
    @Test
    void shapesRuntimeConditions() {
        In few = new In("id", new Object[] {3L, 1L, 2L});
        assertEquals(few, FlightScan.runtimeCondition(few, 1000));

        Filter range = FlightScan.runtimeCondition(new In("k", new Object[] {7, null, -2, 5}), 3);
        assertEquals(new And(new GreaterThanOrEqual("k", -2), new LessThanOrEqual("k", 7)), range);

        Object[] strings = new Object[FlightScan.MAX_RUNTIME_KEYS + 1];
        java.util.Arrays.fill(strings, "x");
        assertNull(FlightScan.runtimeCondition(new In("s", strings), 1000));
        Object[] fewStrings = {"a", "b"};
        assertArrayEquals(fewStrings, ((In) FlightScan.runtimeCondition(new In("s", fewStrings), 1)).values());

        assertNull(FlightScan.runtimeCondition(new EqualTo("id", 1L), 1000));
        assertNull(FlightScan.runtimeCondition(new In("id", new Object[0]), 1000));
    }

    /** Runtime filters re-plan the scan with the rendered conditions; nothing usable means no re-plan. */
    @Test
    void filterReplansWithRenderedConditions() {
        Table table = keyedTable(1L << 40);
        when(table.canPushFilter(org.mockito.ArgumentMatchers.any())).thenReturn(true);
        when(table.toWhereClause(org.mockito.ArgumentMatchers.any(Filter.class)))
                .thenAnswer(invocation -> "rendered " + invocation.getArgument(0));
        List<List<String>> replans = new ArrayList<>();
        FlightScan scan = new FlightScan(mock(Configuration.class), table, replans::add, true);

        scan.filter(new Filter[] {new EqualTo("id", 1L)});
        assertTrue(replans.isEmpty());

        In in = new In("id", new Object[] {1L, 2L});
        scan.filter(new Filter[] {in});
        assertEquals(List.of(List.of("rendered " + in)), replans);
        assertEquals(List.of("rendered " + in), scan.runtimeConditions());
        assertTrue(scan.retainsFilters());
    }
}
