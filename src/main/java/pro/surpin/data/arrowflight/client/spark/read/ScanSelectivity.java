package pro.surpin.data.arrowflight.client.spark.read;

import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.Literal;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;

import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;

/**
 * Estimates the fraction of rows that pushed Spark V2 predicates keep, with textbook optimizer defaults refined by
 * the server's column statistics when it has them: footer bounds turn a range comparison into the share of the
 * value range it keeps (and a value outside the bounds into no rows), null counts give {@code IS NULL}, distinct
 * counts give equality. Conditions are taken as independent.
 */
final class ScanSelectivity {
    /** Selectivity of an equality comparison. */
    static final double EQUALITY = 0.1;
    /** Selectivity of a range comparison. */
    static final double RANGE = 1.0 / 3.0;
    /** Upper bound for an IN list without distinct counts. */
    static final double IN_CAP = 0.5;
    /** Selectivity of a string pattern predicate. */
    static final double PATTERN = 0.25;
    private static final long MICROS_PER_SECOND = 1_000_000L;

    /**
     * Prevents instantiation.
     */
    private ScanSelectivity() {
    }

    /**
     * Estimates the combined selectivity of conjunctive predicates without statistics.
     *
     * @param predicates pushed predicates, all of which must hold
     * @return fraction of rows kept, in [0, 1]
     */
    static double of(Predicate[] predicates) {
        return of(predicates, ServerColumnStatistics.Answer.NONE);
    }

    /**
     * Estimates the combined selectivity of conjunctive predicates.
     *
     * @param predicates pushed predicates, all of which must hold
     * @param stats the server's statistics of the table, or {@link ServerColumnStatistics.Answer#NONE}
     * @return fraction of rows kept, in [0, 1]
     */
    static double of(Predicate[] predicates, ServerColumnStatistics.Answer stats) {
        double selectivity = 1.0;
        for (Predicate predicate : predicates) {
            selectivity *= of(predicate, stats);
        }
        return selectivity;
    }

    /**
     * Estimates the selectivity of one predicate without statistics; unknown shapes keep every row.
     *
     * @param predicate pushed predicate
     * @return fraction of rows kept, in [0, 1]
     */
    static double of(Predicate predicate) {
        return of(predicate, ServerColumnStatistics.Answer.NONE);
    }

    /**
     * Estimates the selectivity of one predicate; unknown shapes keep every row.
     *
     * @param predicate pushed predicate
     * @param stats the server's statistics of the table
     * @return fraction of rows kept, in [0, 1]
     */
    static double of(Predicate predicate, ServerColumnStatistics.Answer stats) {
        String name = predicate.name().toUpperCase(Locale.ROOT);
        ServerColumnStatistics.Column column = column(predicate, stats);
        double value = value(predicate);
        return switch (name) {
            case "=", "<=>" -> equality(column, value);
            case "<" -> below(column, value, false);
            case "<=" -> below(column, value, true);
            case ">" -> above(column, value, false);
            case ">=" -> above(column, value, true);
            case "<>" -> 1.0 - EQUALITY;
            case "IN" -> members(predicate, column);
            case "IS_NULL" -> nulls(column, stats.rows(), EQUALITY);
            case "IS_NOT_NULL" -> 1.0 - nulls(column, stats.rows(), EQUALITY);
            case "STARTS_WITH", "ENDS_WITH", "CONTAINS" -> PATTERN;
            case "AND" -> child(predicate, 0, stats) * child(predicate, 1, stats);
            case "OR" -> {
                double left = child(predicate, 0, stats);
                double right = child(predicate, 1, stats);
                yield left + right - left * right;
            }
            case "NOT" -> {
                // An unknown child estimates 1.0; its negation must not become "no rows".
                double inner = child(predicate, 0, stats);
                yield inner >= 1.0 ? 1.0 : 1.0 - inner;
            }
            case "ALWAYS_FALSE" -> 0.0;
            default -> 1.0;
        };
    }

    /**
     * Names the columns whose statistics refine the estimate of the predicates.
     *
     * @param predicates pushed predicates
     * @return column names
     */
    static Set<String> columns(Predicate[] predicates) {
        Set<String> names = new LinkedHashSet<>();
        for (Predicate predicate : predicates) {
            for (NamedReference reference : predicate.references()) {
                names.add(String.join(".", reference.fieldNames()));
            }
        }
        return names;
    }

    /**
     * Estimates a child of a boolean operator; a non-predicate child keeps every row.
     *
     * @param predicate parent predicate
     * @param index child index
     * @param stats the server's statistics of the table
     * @return fraction of rows kept, in [0, 1]
     */
    private static double child(Predicate predicate, int index, ServerColumnStatistics.Answer stats) {
        Expression[] children = predicate.children();
        if (index < children.length && children[index] instanceof Predicate child) {
            return of(child, stats);
        }
        return 1.0;
    }

    /**
     * Returns the statistics of the column a comparison starts with.
     *
     * @param predicate predicate
     * @param stats the server's statistics of the table
     * @return column statistics, or null
     */
    private static ServerColumnStatistics.Column column(Predicate predicate, ServerColumnStatistics.Answer stats) {
        Expression[] children = predicate.children();
        if (children.length > 0 && children[0] instanceof NamedReference reference) {
            return stats.column(String.join(".", reference.fieldNames()));
        }
        return null;
    }

    /**
     * Returns the literal a column comparison compares with, in the unit of the server's bounds.
     *
     * @param predicate predicate
     * @return the value, or NaN when it is not a comparable literal
     */
    private static double value(Predicate predicate) {
        Expression[] children = predicate.children();
        return children.length == 2 && children[1] instanceof Literal<?> literal ? number(literal) : Double.NaN;
    }

    /**
     * Converts a literal to the unit of the server's bounds: numbers and decimals by value, dates as epoch days,
     * timestamps as epoch seconds.
     *
     * @param literal Spark literal
     * @return the value, or NaN for other types (strings, binary, ...)
     */
    static double number(Literal<?> literal) {
        Object value = literal.value();
        DataType type = literal.dataType();
        if (value == null) {
            return Double.NaN;
        }
        if (DataTypes.TimestampType.equals(type) || "timestamp_ntz".equals(type.typeName())) {
            return value instanceof Long micros ? (double) micros / MICROS_PER_SECOND : Double.NaN;
        }
        if (DataTypes.DateType.equals(type)) {
            return value instanceof Integer days ? days.doubleValue() : Double.NaN;
        }
        if (value instanceof Decimal decimal) {
            return decimal.toDouble();
        }
        if (value instanceof Number number && !(DataTypes.BooleanType.equals(type))) {
            return number.doubleValue();
        }
        return Double.NaN;
    }

    /**
     * Estimates an equality comparison.
     *
     * @param column column statistics, or null
     * @param value compared value, or NaN
     * @return fraction kept
     */
    private static double equality(ServerColumnStatistics.Column column, double value) {
        if (column != null && column.hasBounds() && !Double.isNaN(value)
                && (value < column.min() || value > column.max())) {
            return 0.0;
        }
        if (column != null && column.ndv() != null && column.ndv() > 0) {
            return 1.0 / column.ndv();
        }
        return EQUALITY;
    }

    /**
     * Estimates an IN list: each listed value as an equality.
     *
     * @param predicate IN predicate (column first, then the values)
     * @param column column statistics, or null
     * @return fraction kept
     */
    private static double members(Predicate predicate, ServerColumnStatistics.Column column) {
        Expression[] children = predicate.children();
        int count = Math.max(1, children.length - 1);
        if (column == null) {
            return Math.min(IN_CAP, count * EQUALITY);
        }
        double sum = 0.0;
        for (int i = 1; i < children.length; i++) {
            double value = children[i] instanceof Literal<?> literal ? number(literal) : Double.NaN;
            sum += equality(column, value);
        }
        boolean distinct = column.ndv() != null && column.ndv() > 0;
        return Math.min(distinct ? 1.0 : IN_CAP, sum);
    }

    /**
     * Estimates {@code column < value} (or {@code <=}) as the share of the column's value range below the value.
     *
     * @param column column statistics, or null
     * @param value compared value, or NaN
     * @param inclusive true for {@code <=}
     * @return fraction kept
     */
    private static double below(ServerColumnStatistics.Column column, double value, boolean inclusive) {
        if (column == null || !column.hasBounds() || Double.isNaN(value)) {
            return RANGE;
        }
        double min = column.min();
        double max = column.max();
        if (value < min || (!inclusive && value == min)) {
            return 0.0;
        }
        if (value > max || (inclusive && value == max)) {
            return 1.0;
        }
        return (value - min) / (max - min);
    }

    /**
     * Estimates {@code column > value} (or {@code >=}) as the share of the column's value range above the value.
     *
     * @param column column statistics, or null
     * @param value compared value, or NaN
     * @param inclusive true for {@code >=}
     * @return fraction kept
     */
    private static double above(ServerColumnStatistics.Column column, double value, boolean inclusive) {
        if (column == null || !column.hasBounds() || Double.isNaN(value)) {
            return RANGE;
        }
        double min = column.min();
        double max = column.max();
        if (value > max || (!inclusive && value == max)) {
            return 0.0;
        }
        if (value < min || (inclusive && value == min)) {
            return 1.0;
        }
        return (max - value) / (max - min);
    }

    /**
     * Estimates the share of null values.
     *
     * @param column column statistics, or null
     * @param rows table rows, or negative when unknown
     * @param fallback share without statistics
     * @return fraction of nulls
     */
    private static double nulls(ServerColumnStatistics.Column column, long rows, double fallback) {
        if (column == null || column.nulls() < 0 || rows <= 0) {
            return fallback;
        }
        return Math.min(1.0, (double) column.nulls() / rows);
    }
}
