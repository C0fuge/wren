package pro.surpin.data.arrowflight.perf;

import org.apache.spark.sql.RowFactory;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests of {@link TpchPerfTest}'s result comparison, timing and option helpers (the benchmark itself
 * runs only with -Pperf).
 */
@Tag("unit")
class TpchPerfHelpersTest {

    /** Rows match regardless of order, decimal scale, float noise and Spark's scale-6 {@code avg}. */
    @Test
    void rowsMatchIgnoringOrderScaleAndRounding() {
        List<List<Object>> a = TpchPerfTest.canonical(List.of(
                RowFactory.create("B", new BigDecimal("1.50"), 0.1 + 0.2, 7L),
                RowFactory.create("A", new BigDecimal("2"), new BigDecimal("0.049985"), 8L)));
        List<List<Object>> b = TpchPerfTest.canonical(List.of(
                RowFactory.create("A", new BigDecimal("2.000"), 0.04998529584, 8L),
                RowFactory.create("B", new BigDecimal("1.5"), 0.3, 7L)));
        assertTrue(TpchPerfTest.sameRows(a, b));
        assertEquals("A", a.get(0).get(0));
        assertTrue(TpchPerfTest.sameRows(
                TpchPerfTest.canonical(List.of(RowFactory.create(14, new BigDecimal("7843.52")),
                        RowFactory.create(15, new BigDecimal("7843.53")))),
                TpchPerfTest.canonical(List.of(RowFactory.create(new BigDecimal("15"), new BigDecimal("7843.530000000000")),
                        RowFactory.create(14L, new BigDecimal("7843.520000000000"))))));
        assertTrue(TpchPerfTest.sameRows(
                TpchPerfTest.canonical(List.of(RowFactory.create(new BigDecimal("55909065222.827692")))),
                TpchPerfTest.canonical(List.of(RowFactory.create(55909065222.82769)))));
    }

    /** A real difference (other value, other integer, missing row, null) is not tolerated. */
    @Test
    void rowsKeepDifferences() {
        List<List<Object>> reference =
                TpchPerfTest.canonical(List.of(RowFactory.create("A", 1L, new BigDecimal("10.01"))));
        assertFalse(TpchPerfTest.sameRows(reference,
                TpchPerfTest.canonical(List.of(RowFactory.create("A", 1L, new BigDecimal("10.02"))))));
        assertFalse(TpchPerfTest.sameRows(reference,
                TpchPerfTest.canonical(List.of(RowFactory.create("A", 2L, new BigDecimal("10.01"))))));
        assertFalse(TpchPerfTest.sameRows(reference,
                TpchPerfTest.canonical(List.of(RowFactory.create("A", 1L, null)))));
        assertFalse(TpchPerfTest.sameRows(reference, TpchPerfTest.canonical(List.of())));
        assertFalse(TpchPerfTest.sameRows(reference,
                TpchPerfTest.canonical(List.of(RowFactory.create("A", new BigDecimal("1.0000001"),
                        new BigDecimal("10.01"))))));
        assertEquals("row 0: expected [A (String), 1 (Long), 10.01 (BigDecimal)], got [A (String), 2 (Long), "
                + "10.01 (BigDecimal)]", TpchPerfTest.firstDifference(reference,
                TpchPerfTest.canonical(List.of(RowFactory.create("A", 2, new BigDecimal("10.01"))))));
        assertFalse(TpchPerfTest.sameRows(
                TpchPerfTest.canonical(List.of(RowFactory.create(new BigDecimal("56586554400.73")))),
                TpchPerfTest.canonical(List.of(RowFactory.create(new BigDecimal("56586554400.83"))))));
    }

    /** Median of odd and even run counts (upper median). */
    @Test
    void median() {
        assertEquals(5, TpchPerfTest.median(new long[] {9, 5, 1}));
        assertEquals(7, TpchPerfTest.median(new long[] {7, 3}));
        assertEquals(4, TpchPerfTest.median(new long[] {4}));
    }

    /** The Parquet reference always runs first and once. */
    @Test
    void modesStartWithReference() {
        assertEquals(List.of("parquet", "flight", "jdbc"), TpchPerfTest.modes("flight, jdbc"));
        assertEquals(List.of("parquet", "flight"), TpchPerfTest.modes("flight,parquet,flight,"));
    }

    /** {@code all} selects the 22 queries, {@code probes} the probe scans; a list selects those. */
    @Test
    void querySelection() {
        assertEquals(22, TpchPerfTest.queries("all").size());
        assertEquals("q22", TpchPerfTest.queries("all").get(21));
        assertEquals(List.of("q01", "q06", "q14"), TpchPerfTest.queries("1, 6,14"));
        assertEquals(List.of("p01", "p02", "p03"), TpchPerfTest.queries("probes"));
        assertEquals(List.of("q05", "p01", "q13"), TpchPerfTest.queries("5,P1,q13"));
    }

    /** Summary: medians, a failed query, totals over the queries that ran, ratios to the reference. */
    @Test
    void summaryTable() {
        Map<String, Map<String, long[]>> times = new LinkedHashMap<>();
        times.put("parquet", new LinkedHashMap<>(Map.of("q01", new long[] {300, 100, 200}, "q06", new long[] {50})));
        times.put("flight", new LinkedHashMap<>(Map.of("q01", new long[] {100, 100, 100})));
        String md = TpchPerfTest.summary(times, List.of("q01", "q06"), 3);
        assertTrue(md.contains("| query | parquet | flight | flight/parquet |\n|---|---|---|---|\n"), md);
        assertTrue(md.contains("| q01 | 200 | 100 | 0.50 |"), md);
        assertTrue(md.contains("| q06 | 50 | failed | - |"), md);
        assertTrue(md.contains("| total | 250 | 100 | 0.40 |"), md);
    }
}
