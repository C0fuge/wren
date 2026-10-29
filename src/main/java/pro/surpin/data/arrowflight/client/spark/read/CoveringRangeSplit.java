package pro.surpin.data.arrowflight.client.spark.read;

import pro.surpin.data.arrowflight.client.model.Field;
import pro.surpin.data.arrowflight.client.write.PartitionBehavior;

import java.io.Serial;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Partitioning of a scan ({@code partition.*} options) whose range split covers every row exactly once. The client
 * core splits {@code [lowerBound, upperBound)} into {@code size} slices {@code lo <= col and col < hi}, so rows below
 * the lower bound, at or above the upper bound, or with a null key were silently dropped, and bounds narrower than
 * {@code size} values produced the same {@code col = v} slice several times (duplicate rows). This keeps the client
 * core's slice edges (it parses the bounds for the column's type) and rewrites the slices as Spark's JDBC source
 * does: the bounds only decide the stride, the first slice also takes everything below its upper edge and the null
 * keys, the last everything from its lower edge, and equal edges collapse. Hash splits and explicit
 * {@code partition.predicates} pass through unchanged.
 */
public class CoveringRangeSplit extends PartitionBehavior {

    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * Creates the partitioning, with the arguments of {@link PartitionBehavior}.
     *
     * @param hashFunc server function for hash splits
     * @param byColumn split column, or empty
     * @param size number of slices
     * @param lowerBound lower bound of a range split, or empty
     * @param upperBound upper bound of a range split, or empty
     * @param predicates explicit partition predicates (one partition each)
     */
    public CoveringRangeSplit(String hashFunc, String byColumn, int size, String lowerBound, String upperBound,
                              String[] predicates) {
        super(hashFunc, byColumn, size, lowerBound, upperBound, predicates);
    }

    /**
     * Returns the client core's predicates, with range slices rewritten to cover every row once.
     *
     * @param dataFields fields of the scanned relation
     * @return one predicate per partition
     */
    @Override
    public String[] calculatePredicates(Field[] dataFields) {
        String[] slices = super.calculatePredicates(dataFields);
        List<String> edges = rangeEdges(getByColumn(), slices);
        return edges == null ? slices : covering(getByColumn(), edges);
    }

    /**
     * Reads the edges of the client core's range slices ({@code lo <= col and col < hi} or {@code col = v}), equal
     * neighbours collapsed.
     *
     * @param column split column as written in the slices
     * @param slices the client core's predicates
     * @return ascending distinct edges, or null when the slices are no range split
     */
    static List<String> rangeEdges(String column, String[] slices) {
        if (column == null || column.isEmpty() || slices == null || slices.length == 0) {
            return null;
        }
        String col = Pattern.quote(column);
        Pattern range = Pattern.compile("^(.+) <= " + col + " and " + col + " < (.+)$");
        Pattern single = Pattern.compile("^" + col + " = (.+)$");
        List<String> edges = new ArrayList<>();
        for (String slice : slices) {
            Matcher r = range.matcher(slice);
            Matcher s = single.matcher(slice);
            String lo;
            String hi;
            if (r.matches()) {
                lo = r.group(1);
                hi = r.group(2);
            } else if (s.matches()) {
                lo = s.group(1);
                hi = lo;
            } else {
                return null;
            }
            if (edges.isEmpty()) {
                edges.add(lo);
            }
            if (!edges.get(edges.size() - 1).equals(hi)) {
                edges.add(hi);
            }
        }
        return edges;
    }

    /**
     * Builds slices from the inner edges: the first open below (and with the null keys), the last open above.
     *
     * @param column split column
     * @param edges ascending distinct edges, the outer two being the bounds
     * @return predicates covering every row exactly once
     */
    static String[] covering(String column, List<String> edges) {
        List<String> inner = edges.subList(1, Math.max(1, edges.size() - 1));
        if (inner.isEmpty()) {
            return new String[] {String.format("%s is null or %s is not null", column, column)};
        }
        String[] predicates = new String[inner.size() + 1];
        predicates[0] = String.format("%s < %s or %s is null", column, inner.get(0), column);
        for (int i = 1; i < inner.size(); i++) {
            predicates[i] = String.format("%s <= %s and %s < %s", inner.get(i - 1), column, column, inner.get(i));
        }
        predicates[inner.size()] = String.format("%s >= %s", column, inner.get(inner.size() - 1));
        return predicates;
    }
}
