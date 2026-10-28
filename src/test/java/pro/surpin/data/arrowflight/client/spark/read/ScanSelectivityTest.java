package pro.surpin.data.arrowflight.client.spark.read;

import pro.surpin.data.arrowflight.client.Configuration;
import pro.surpin.data.arrowflight.client.model.Table;
import pro.surpin.data.arrowflight.client.query.PushAggregation;
import pro.surpin.data.arrowflight.client.write.PartitionBehavior;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests the selectivity estimate of pushed predicates and how the scan builder hands it to the table.
 */
@Tag("unit")
class ScanSelectivityTest {
    private static final double EPS = 1e-9;

    /**
     * Builds a column-literal comparison.
     *
     * @param op predicate name
     * @return predicate
     */
    private static Predicate compare(String op) {
        return new Predicate(op, new Expression[]{
                FieldReference.column("id"), LiteralValue.apply(1, DataTypes.IntegerType)});
    }

    /**
     * Builds a predicate over column {@code id} that the estimator does not know.
     *
     * @return unknown predicate
     */
    private static Predicate unknown() {
        return new Predicate("SOME_UDF", new Expression[]{FieldReference.column("id")});
    }

    /** Verifies the textbook defaults for single comparisons. */
    @Test
    void comparisonsUseOptimizerDefaults() {
        assertEquals(0.1, ScanSelectivity.of(compare("=")), EPS);
        assertEquals(0.1, ScanSelectivity.of(compare("<=>")), EPS);
        assertEquals(1.0 / 3.0, ScanSelectivity.of(compare("<")), EPS);
        assertEquals(1.0 / 3.0, ScanSelectivity.of(compare(">=")), EPS);
        assertEquals(0.9, ScanSelectivity.of(compare("<>")), EPS);
        assertEquals(1.0, ScanSelectivity.of(unknown()), EPS);
    }

    /** Verifies IN lists scale with their length and are capped. */
    @Test
    void inListScalesAndIsCapped() {
        Predicate three = new Predicate("IN", new Expression[]{FieldReference.column("id"),
                LiteralValue.apply(1, DataTypes.IntegerType), LiteralValue.apply(2, DataTypes.IntegerType),
                LiteralValue.apply(3, DataTypes.IntegerType)});
        assertEquals(0.3, ScanSelectivity.of(three), EPS);
        Expression[] many = new Expression[21];
        many[0] = FieldReference.column("id");
        for (int i = 1; i < many.length; i++) {
            many[i] = LiteralValue.apply(i, DataTypes.IntegerType);
        }
        assertEquals(0.5, ScanSelectivity.of(new Predicate("IN", many)), EPS);
    }

    /** Verifies AND multiplies, OR uses inclusion-exclusion and the top-level array is conjunctive. */
    @Test
    void booleanOperatorsCombine() {
        Predicate and = new Predicate("AND", new Expression[]{compare("="), compare("<")});
        Predicate or = new Predicate("OR", new Expression[]{compare("="), compare("=")});
        assertEquals(0.1 / 3.0, ScanSelectivity.of(and), EPS);
        assertEquals(0.19, ScanSelectivity.of(or), EPS);
        assertEquals(0.01, ScanSelectivity.of(new Predicate[]{compare("="), compare("=")}), EPS);
        assertEquals(1.0, ScanSelectivity.of(new Predicate[0]), EPS);
    }

    /** Verifies negation, and that negating an unknown predicate never estimates an empty scan. */
    @Test
    void negationOfUnknownKeepsEveryRow() {
        assertEquals(0.9, ScanSelectivity.of(new Predicate("NOT", new Expression[]{compare("=")})), EPS);
        // Upstream's 1 - 1.0 made this 0 rows: Spark would broadcast a scan of the whole table.
        assertEquals(1.0, ScanSelectivity.of(new Predicate("NOT", new Expression[]{unknown()})), EPS);
        Predicate orUnknown = new Predicate("OR", new Expression[]{compare("="), unknown()});
        assertEquals(1.0, ScanSelectivity.of(new Predicate("NOT", new Expression[]{orUnknown})), EPS);
    }

    /** Verifies only an aggregation with functions and no grouping keys counts as global. */
    @Test
    void globalAggregationNeedsFunctionsAndNoGroups() {
        assertTrue(FlightScanBuilder.isGlobalAggregation(new PushAggregation(new String[]{"sum(x)"})));
        assertTrue(FlightScanBuilder.isGlobalAggregation(
                new PushAggregation(new String[]{"sum(x)"}, new String[0])));
        assertFalse(FlightScanBuilder.isGlobalAggregation(
                new PushAggregation(new String[]{"sum(x)"}, new String[]{"g"})));
        assertFalse(FlightScanBuilder.isGlobalAggregation(new PushAggregation(new String[0])));
        assertFalse(FlightScanBuilder.isGlobalAggregation(null));
    }

    /** Verifies the filter selectivity reaches the table only when the read option enables it. */
    @Test
    void builderAppliesFilterSelectivityOnlyWhenEnabled() {
        for (boolean enabled : new boolean[]{false, true}) {
            Table table = mock(Table.class);
            when(table.canPushPredicate(any())).thenReturn(true);
            when(table.toWhereClause(any(Predicate.class))).thenReturn("id = 1");
            FlightScanBuilder builder = new FlightScanBuilder(mock(Configuration.class), table,
                    new PartitionBehavior(null, null, 1, null, null, null), enabled);
            builder.pushPredicates(new Predicate[]{compare("=")});

            builder.build();

            verify(table).setPushdownEstimate(enabled ? 0.1 : 1.0, false);
            verify(table).initialize(any());
        }
    }

    /** Statistics of {@code id} in [0, 100] with 5 nulls in 200 rows and no distinct count, as kestrel sends. */
    private static ServerColumnStatistics.Answer idStats(Long ndv) {
        return new ServerColumnStatistics.Answer(200,
                Map.of("ID", new ServerColumnStatistics.Column(0.0, 100.0, 5, ndv)));
    }

    private static Predicate compare(String op, Object value, org.apache.spark.sql.types.DataType type) {
        return new Predicate(op, new Expression[]{
                FieldReference.column("id"), LiteralValue.apply(value, type)});
    }

    /** Verifies range comparisons use the share of the value range, inclusive or not, and out-of-range values. */
    @Test
    void rangeComparisonsUseBounds() {
        ServerColumnStatistics.Answer stats = idStats(null);
        assertEquals(0.25, ScanSelectivity.of(compare("<", 25, DataTypes.IntegerType), stats), EPS);
        assertEquals(0.75, ScanSelectivity.of(compare(">=", 25L, DataTypes.LongType), stats), EPS);
        assertEquals(0.0, ScanSelectivity.of(compare(">", 100, DataTypes.IntegerType), stats), EPS);
        assertEquals(1.0, ScanSelectivity.of(compare("<=", 100, DataTypes.IntegerType), stats), EPS);
        assertEquals(0.0, ScanSelectivity.of(compare("<", 0, DataTypes.IntegerType), stats), EPS);
        assertEquals(1.0, ScanSelectivity.of(compare(">=", 0, DataTypes.IntegerType), stats), EPS);
        assertEquals(1.0, ScanSelectivity.of(compare(">", -5.5, DataTypes.DoubleType), stats), EPS);
        assertEquals(0.5, ScanSelectivity.of(compare("<", Decimal.apply("50.00"), DataTypes.createDecimalType(4, 2)),
                stats), EPS);
        // Strings have no bounds: the default stays.
        assertEquals(1.0 / 3.0, ScanSelectivity.of(compare("<", UTF8String.fromString("m"), DataTypes.StringType),
                stats), EPS);
        // A column the server did not describe keeps the default too.
        Predicate other = new Predicate("<", new Expression[]{
                FieldReference.column("other"), LiteralValue.apply(1, DataTypes.IntegerType)});
        assertEquals(1.0 / 3.0, ScanSelectivity.of(other, stats), EPS);
    }

    /** Verifies a single-value column: comparisons are all or nothing. */
    @Test
    void singleValueColumn() {
        ServerColumnStatistics.Answer stats = new ServerColumnStatistics.Answer(10,
                Map.of("id", new ServerColumnStatistics.Column(7.0, 7.0, 0, null)));
        assertEquals(1.0, ScanSelectivity.of(compare(">=", 7, DataTypes.IntegerType), stats), EPS);
        assertEquals(0.0, ScanSelectivity.of(compare(">", 7, DataTypes.IntegerType), stats), EPS);
        assertEquals(1.0, ScanSelectivity.of(compare("<=", 7, DataTypes.IntegerType), stats), EPS);
        assertEquals(0.0, ScanSelectivity.of(compare("<", 7, DataTypes.IntegerType), stats), EPS);
    }

    /** Verifies equality and IN: out-of-range values keep nothing, distinct counts replace the default. */
    @Test
    void equalityUsesBoundsAndDistinctCounts() {
        assertEquals(0.0, ScanSelectivity.of(compare("=", 101, DataTypes.IntegerType), idStats(null)), EPS);
        assertEquals(0.1, ScanSelectivity.of(compare("=", 50, DataTypes.IntegerType), idStats(null)), EPS);
        assertEquals(0.02, ScanSelectivity.of(compare("=", 50, DataTypes.IntegerType), idStats(50L)), EPS);
        Predicate in = new Predicate("IN", new Expression[]{FieldReference.column("id"),
                LiteralValue.apply(1, DataTypes.IntegerType), LiteralValue.apply(2, DataTypes.IntegerType),
                LiteralValue.apply(500, DataTypes.IntegerType)});
        assertEquals(0.2, ScanSelectivity.of(in, idStats(null)), EPS);
        assertEquals(0.04, ScanSelectivity.of(in, idStats(50L)), EPS);
    }

    /** Verifies IS NULL / IS NOT NULL use the null count. */
    @Test
    void nullChecksUseNullCount() {
        Predicate isNull = new Predicate("IS_NULL", new Expression[]{FieldReference.column("id")});
        Predicate notNull = new Predicate("IS_NOT_NULL", new Expression[]{FieldReference.column("id")});
        assertEquals(0.025, ScanSelectivity.of(isNull, idStats(null)), EPS);
        assertEquals(0.975, ScanSelectivity.of(notNull, idStats(null)), EPS);
        assertEquals(0.1, ScanSelectivity.of(isNull), EPS);
    }

    /** Verifies dates compare as epoch days and timestamps as epoch seconds, the units of the server's bounds. */
    @Test
    void literalsUseTheUnitsOfTheBounds() {
        assertEquals(19000.0, ScanSelectivity.number(LiteralValue.apply(19000, DataTypes.DateType)), EPS);
        assertEquals(1.5, ScanSelectivity.number(LiteralValue.apply(1_500_000L, DataTypes.TimestampType)), EPS);
        assertEquals(2.5, ScanSelectivity.number(LiteralValue.apply(Decimal.apply("2.5"),
                DataTypes.createDecimalType(3, 1))), EPS);
        assertTrue(Double.isNaN(ScanSelectivity.number(LiteralValue.apply(true, DataTypes.BooleanType))));
        assertTrue(Double.isNaN(ScanSelectivity.number(LiteralValue.apply(null, DataTypes.IntegerType))));
    }

    /** Verifies the statistics columns are the predicates' references, and combined estimates. */
    @Test
    void columnsAndCombinations() {
        Predicate and = new Predicate("AND", new Expression[]{compare("<", 50, DataTypes.IntegerType),
                new Predicate("=", new Expression[]{FieldReference.column("name"),
                        LiteralValue.apply(UTF8String.fromString("x"), DataTypes.StringType)})});
        assertEquals(java.util.List.of("id", "name"),
                java.util.List.copyOf(ScanSelectivity.columns(new Predicate[]{and})));
        assertEquals(0.05, ScanSelectivity.of(new Predicate[]{and}, idStats(null)), EPS);
        Predicate not = new Predicate("NOT", new Expression[]{compare(">", 200, DataTypes.IntegerType)});
        assertEquals(1.0, ScanSelectivity.of(not, idStats(null)), EPS);
    }

    /** Verifies the server's JSON answer parses (unknown fields ignored, unknown values as null / -1). */
    @Test
    void parsesServerAnswer() throws Exception {
        ServerColumnStatistics.Answer answer = ServerColumnStatistics.parse("{\"rows\":1000,\"rowsExact\":true,"
                + "\"bytes\":123,\"files\":1,\"columns\":{\"id\":{\"min\":0.0,\"max\":999.0,\"nulls\":0,"
                + "\"bytes\":4000,\"ndv\":null},\"string_col\":{\"min\":null,\"max\":null,\"nulls\":-1,"
                + "\"bytes\":9000,\"ndv\":null}}}");
        assertEquals(1000L, answer.rows());
        assertEquals(new ServerColumnStatistics.Column(0.0, 999.0, 0, null), answer.column("ID"));
        assertFalse(answer.column("string_col").hasBounds());
        assertEquals(null, answer.column("missing"));
        assertEquals(null, ServerColumnStatistics.Answer.NONE.column("id"));
    }

    /** Verifies only plain {@code schema.table} sources are sent to the statistics action. */
    @Test
    void onlyPlainTablesAreDescribable() {
        assertTrue(ServerColumnStatistics.describable("test_schema.test_table"));
        assertFalse(ServerColumnStatistics.describable("test_table"));
        assertFalse(ServerColumnStatistics.describable("\"a b\".t"));
        assertFalse(ServerColumnStatistics.describable("(select 1)"));
        assertFalse(ServerColumnStatistics.describable("a.b.c"));
        assertFalse(ServerColumnStatistics.describable(null));
        assertEquals(ServerColumnStatistics.Answer.NONE,
                ServerColumnStatistics.fetch(mock(Configuration.class), "nope", java.util.List.of("id")));
    }
}
