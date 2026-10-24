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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

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
}
