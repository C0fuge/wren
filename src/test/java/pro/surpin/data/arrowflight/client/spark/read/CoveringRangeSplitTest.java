package pro.surpin.data.arrowflight.client.spark.read;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import pro.surpin.data.arrowflight.client.model.Field;
import pro.surpin.data.arrowflight.client.model.FieldType;
import pro.surpin.data.arrowflight.client.write.PartitionBehavior;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Unit tests of {@link CoveringRangeSplit}: range slices cover every row once, other splits pass through.
 */
@Tag("unit")
class CoveringRangeSplitTest {

    private static Field[] fields(String name, FieldType.IDs type) {
        return new Field[] {new Field(name, new FieldType(type))};
    }

    private static Field[] bigint(String name) {
        return fields(name, FieldType.IDs.BIGINT);
    }

    private static String[] split(String column, int size, String lower, String upper, Field[] fields) {
        return new CoveringRangeSplit("hash", column, size, lower, upper, new String[0]).calculatePredicates(fields);
    }

    @Test
    void longRangeIsOpenAtBothEndsAndTakesNulls() {
        assertArrayEquals(new String[] {
            "k < 25 or k is null",
            "25 <= k and k < 50",
            "50 <= k and k < 75",
            "k >= 75"
        }, split("k", 4, "0", "100", bigint("k")));
    }

    /** Bounds narrower than the slice count gave the client core's {@code k = v} slice several times. */
    @Test
    void narrowBoundsGiveNoRepeatedSlices() {
        String[] core = new PartitionBehavior("hash", "k", 6, "0", "2", new String[0]).calculatePredicates(bigint("k"));
        assertEquals("k = 0", core[0]);
        assertEquals("k = 0", core[1]);
        assertArrayEquals(new String[] {"k < 1 or k is null", "k >= 1"}, split("k", 6, "0", "2", bigint("k")));
    }

    @Test
    void singleSliceReadsEverything() {
        assertArrayEquals(new String[] {"k is null or k is not null"}, split("k", 1, "0", "100", bigint("k")));
        assertArrayEquals(new String[] {"k is null or k is not null"}, split("k", 3, "5", "5", bigint("k")));
    }

    @Test
    void doubleAndDateRangesKeepTheCoreLiterals() {
        assertArrayEquals(new String[] {"d < 5.0 or d is null", "d >= 5.0"},
                split("d", 2, "0", "10", fields("d", FieldType.IDs.DOUBLE)));
        assertArrayEquals(new String[] {"day < '2020-01-03' or day is null", "day >= '2020-01-03'"},
                split("day", 2, "2020-01-01", "2020-01-05", fields("day", FieldType.IDs.DATE)));
    }

    @Test
    void hashSplitAndExplicitPredicatesPassThrough() {
        String[] hash = split("k", 2, "", "", bigint("k"));
        assertArrayEquals(new PartitionBehavior("hash", "k", 2, "", "", new String[0]).calculatePredicates(bigint("k")),
                hash);
        String[] given = {"k < 10", "k >= 10"};
        assertArrayEquals(given, new CoveringRangeSplit("hash", "", 6, "", "", given).getPredicates());
    }

    @Test
    void rangeEdgesRejectOtherShapes() {
        assertNull(CoveringRangeSplit.rangeEdges("k", new String[] {"(hash(k) % 2 + 2) % 2 = 0"}));
        assertNull(CoveringRangeSplit.rangeEdges("", new String[] {"k = 1"}));
        assertEquals(List.of("1", "2", "3"),
                CoveringRangeSplit.rangeEdges("k", new String[] {"1 <= k and k < 2", "k = 2", "2 <= k and k < 3"}));
    }
}
