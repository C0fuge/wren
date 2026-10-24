package pro.surpin.data.arrowflight.client.spark.read;

import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.filter.Predicate;

import java.util.Locale;

/**
 * Estimates the fraction of rows that pushed Spark V2 predicates keep, with textbook optimizer defaults.
 */
final class ScanSelectivity {
    /** Selectivity of an equality comparison. */
    static final double EQUALITY = 0.1;
    /** Selectivity of a range comparison. */
    static final double RANGE = 1.0 / 3.0;
    /** Upper bound for an IN list. */
    static final double IN_CAP = 0.5;
    /** Selectivity of a string pattern predicate. */
    static final double PATTERN = 0.25;

    /**
     * Prevents instantiation.
     */
    private ScanSelectivity() {
    }

    /**
     * Estimates the combined selectivity of conjunctive predicates.
     *
     * @param predicates pushed predicates, all of which must hold
     * @return fraction of rows kept, in [0, 1]
     */
    static double of(Predicate[] predicates) {
        double selectivity = 1.0;
        for (Predicate predicate : predicates) {
            selectivity *= of(predicate);
        }
        return selectivity;
    }

    /**
     * Estimates the selectivity of one predicate; unknown shapes keep every row.
     *
     * @param predicate pushed predicate
     * @return fraction of rows kept, in [0, 1]
     */
    static double of(Predicate predicate) {
        String name = predicate.name().toUpperCase(Locale.ROOT);
        return switch (name) {
            case "=", "<=>", "IS_NULL" -> EQUALITY;
            case "<", "<=", ">", ">=" -> RANGE;
            case "<>" -> 1.0 - EQUALITY;
            case "IN" -> Math.min(IN_CAP, Math.max(1, predicate.children().length - 1) * EQUALITY);
            case "IS_NOT_NULL" -> 1.0 - EQUALITY;
            case "STARTS_WITH", "ENDS_WITH", "CONTAINS" -> PATTERN;
            case "AND" -> child(predicate, 0) * child(predicate, 1);
            case "OR" -> {
                double left = child(predicate, 0);
                double right = child(predicate, 1);
                yield left + right - left * right;
            }
            case "NOT" -> {
                // An unknown child estimates 1.0; its negation must not become "no rows".
                double inner = child(predicate, 0);
                yield inner >= 1.0 ? 1.0 : 1.0 - inner;
            }
            case "ALWAYS_FALSE" -> 0.0;
            default -> 1.0;
        };
    }

    /**
     * Estimates a child of a boolean operator; a non-predicate child keeps every row.
     *
     * @param predicate parent predicate
     * @param index child index
     * @return fraction of rows kept, in [0, 1]
     */
    private static double child(Predicate predicate, int index) {
        Expression[] children = predicate.children();
        if (index < children.length && children[index] instanceof Predicate child) {
            return of(child);
        }
        return 1.0;
    }
}
