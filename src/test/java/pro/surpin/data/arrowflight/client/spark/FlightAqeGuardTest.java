package pro.surpin.data.arrowflight.client.spark;

import pro.surpin.data.arrowflight.client.spark.read.FlightScan;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests which plans the AQE guard switches adaptive execution off for.
 */
class FlightAqeGuardTest {

    private static FlightScan scan(String table, boolean retained) {
        FlightScan scan = mock(FlightScan.class);
        when(scan.tableName()).thenReturn(table);
        when(scan.retainsFilters()).thenReturn(retained);
        return scan;
    }

    /** Only a plan that scans a table twice and keeps a filter needs AQE off. */
    @Test
    void needsAqeOffForRepeatedTableWithRetainedFilter() {
        assertTrue(FlightAqeGuard.needsAqeOff(List.of(scan("s.t", true), scan("s.t", false))));
        assertFalse(FlightAqeGuard.needsAqeOff(List.of(scan("s.t", false), scan("s.t", false))));
        assertFalse(FlightAqeGuard.needsAqeOff(List.of(scan("s.t", true), scan("s.u", false))));
        assertFalse(FlightAqeGuard.needsAqeOff(List.of(scan("s.t", true))));
        FlightScan filtering = scan("s.t", true);
        assertFalse(FlightAqeGuard.needsAqeOff(List.of(filtering, filtering, scan("s.u", false))),
                "the pruning subquery repeats the same scan");
    }
}
