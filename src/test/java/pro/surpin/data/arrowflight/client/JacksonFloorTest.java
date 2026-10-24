package pro.surpin.data.arrowflight.client;

import com.fasterxml.jackson.core.Version;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Guards the Jackson floor of the connector classpath (docs/dev/DEPENDENCY-AUDIT.md); the
 * Scala module of the provided Spark must match it or Spark refuses to start.
 */
@Tag("unit")
class JacksonFloorTest {

    @Test
    void jacksonModulesShareOneVersionAtTheFloor() {
        List<Version> modules = List.of(
                com.fasterxml.jackson.core.json.PackageVersion.VERSION,
                com.fasterxml.jackson.databind.cfg.PackageVersion.VERSION,
                com.fasterxml.jackson.datatype.jsr310.PackageVersion.VERSION,
                com.fasterxml.jackson.module.scala.DefaultScalaModule$.MODULE$.version());
        Set<String> versions = new TreeSet<>();
        modules.forEach(v -> versions.add(v.toString()));
        assertEquals(1, versions.size(), "one Jackson version for every module: " + versions);
        Version version = modules.get(0);
        assertTrue(atLeast(version, 2, 21, 7),
                "Jackson " + version + " is below 2.21.7");
    }

    @Test
    void versionComparison() {
        assertTrue(atLeast(new Version(2, 21, 7, null, null, null), 2, 21, 7));
        assertTrue(atLeast(new Version(2, 22, 0, null, null, null), 2, 21, 7));
        assertFalse(atLeast(new Version(2, 21, 0, null, "com.fasterxml.jackson.core", "jackson-core"), 2, 21, 7));
        assertFalse(atLeast(new Version(2, 15, 3, null, null, null), 2, 21, 7));
    }

    /**
     * Tells whether a Jackson version is at least the given one ({@link Version#compareTo} also
     * compares group and artifact ids, so it cannot be used against a bare number).
     *
     * @param version version to check
     * @param major lowest major
     * @param minor lowest minor
     * @param patch lowest patch
     * @return true when the version is not below major.minor.patch
     */
    private static boolean atLeast(Version version, int major, int minor, int patch) {
        if (version.getMajorVersion() != major) {
            return version.getMajorVersion() > major;
        }
        if (version.getMinorVersion() != minor) {
            return version.getMinorVersion() > minor;
        }
        return version.getPatchLevel() >= patch;
    }
}
