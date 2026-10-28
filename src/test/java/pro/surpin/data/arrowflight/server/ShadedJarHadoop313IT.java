package pro.surpin.data.arrowflight.server;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * The shaded connector jar on a Hadoop 3.1.3 client class path (the production cluster's Hadoop):
 * a separate JVM runs Spark from {@code target/hadoop-3.1.3.classpath} (a "Hadoop free" Spark of
 * this build line plus the Hadoop 3.1.3 client and its Guava 27 / protobuf 2.5, resolved from
 * {@code src/test/hadoop-3.1.3/pom.xml} by the build) with only {@code target/*-shaded.jar} added
 * after it, as {@code --jars} would, and reads a table from the in-process server through the
 * "flight" source ({@code src/test/hadoop-3.1.3/ShadedJarProbe.java}). Fails on any
 * {@code LinkageError}, {@code NoSuchMethodError} or wrong result. Also checks that the jar holds
 * no unrelocated copy of what Spark or Hadoop ship. Run by failsafe after {@code package}.
 */
@Tag("integration")
@Tag("spark")
@Tag("shaded")
class ShadedJarHadoop313IT {

    private static TestFlightServerHelper helper;

    @BeforeAll
    static void startServer() throws Exception {
        helper = TestFlightServerHelper.builder()
                .dataDir(Paths.get("src/test/resources/test_db").toAbsolutePath().normalize().toString())
                .start();
    }

    @AfterAll
    static void stopServer() throws Exception {
        if (helper != null) helper.close();
    }

    /** Packages the shaded jar must relocate (Spark or Hadoop 3.1.3 ship their own versions). */
    private static final List<String> RELOCATED = List.of(
            "com/fasterxml/jackson/", "org/apache/arrow/", "com/google/flatbuffers/", "io/netty/", "io/grpc/",
            "io/perfmark/", "com/google/protobuf/", "com/google/common/", "com/google/thirdparty/",
            "com/google/gson/", "org/apache/commons/", "org/slf4j/");

    @Test
    void shadedJarRelocatesWhatSparkAndHadoopShip() throws Exception {
        List<String> unrelocated = new ArrayList<>();
        List<String> nativeLibs = new ArrayList<>();
        try (JarFile jar = new JarFile(shadedJar().toFile())) {
            jar.stream().map(JarEntry::getName).forEach(name -> {
                // Multi-release entries (META-INF/versions/<n>/...) count as their class.
                String cls = name.replaceFirst("^META-INF/versions/\\d+/", "");
                if (RELOCATED.stream().anyMatch(cls::startsWith)) unrelocated.add(name);
                if (name.startsWith("META-INF/native/") && !name.endsWith("/")) nativeLibs.add(name);
            });
        }
        assertTrue(unrelocated.isEmpty(), "not relocated: " + unrelocated.subList(0, Math.min(20, unrelocated.size())));
        assertFalse(nativeLibs.isEmpty(), "Netty native libraries expected");
        nativeLibs.forEach(lib -> assertTrue(lib.contains("pro_surpin_data_arrowflight_shaded_netty_"),
                "native library not renamed for the relocated Netty: " + lib));
    }

    @Test
    void shadedJarReadsTableNextToHadoop313() throws Exception {
        Path target = Paths.get("target");
        Path shaded = shadedJar();
        String hadoopClasspath = Files.readString(target.resolve("hadoop-3.1.3.classpath"), StandardCharsets.UTF_8).trim();
        assertTrue(hadoopClasspath.contains("hadoop-common-3.1.3.jar"), "Hadoop 3.1.3 class path expected");

        List<String> cmd = new ArrayList<>(List.of(
                Paths.get(System.getProperty("java.home"), "bin", "java").toString(),
                "--add-opens=java.base/java.nio=ALL-UNNAMED",
                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
                "--add-opens=java.base/java.lang=ALL-UNNAMED",
                "--add-opens=java.base/java.util=ALL-UNNAMED",
                "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
                "-cp", hadoopClasspath + File.pathSeparator + shaded.toAbsolutePath(),
                Paths.get("src/test/hadoop-3.1.3/ShadedJarProbe.java").toAbsolutePath().toString(),
                "localhost", String.valueOf(helper.location.getUri().getPort()), "test_schema.test_table"));
        ProcessBuilder pb = new ProcessBuilder(cmd).redirectErrorStream(true);
        pb.environment().put("SPARK_USER", System.getProperty("user.name", "root"));
        Path log = target.resolve("shaded-jar-hadoop-3.1.3.log");
        pb.redirectOutput(log.toFile());
        Process p = pb.start();
        if (!p.waitFor(3, TimeUnit.MINUTES)) {
            p.destroyForcibly();
            fail("probe did not finish in 3 minutes; see " + log);
        }
        List<String> out = Files.readAllLines(log, StandardCharsets.UTF_8);
        String probe = out.stream().filter(l -> l.startsWith("PROBE ")).collect(Collectors.joining("\n"));
        String errors = out.stream()
                .filter(l -> l.contains("LinkageError") || l.contains("NoSuchMethodError")
                        || l.contains("NoClassDefFoundError") || l.contains("IncompatibleClassChangeError"))
                .collect(Collectors.joining("\n"));
        assertEquals(0, p.exitValue(), "probe failed; see " + log + ":\n" + tail(out));
        assertTrue(errors.isEmpty(), "linkage errors in " + log + ":\n" + errors);
        List<String> lines = out.stream().filter(l -> l.startsWith("PROBE ")).toList();
        assertTrue(lines.stream().anyMatch(l -> l.startsWith("PROBE hadoop=3.1.3 ")), probe);
        // test_db.test_table: id 0..999, tinyint_col 0..9; the connector came from the shaded jar.
        assertTrue(lines.contains("PROBE all=1000,499500"), probe);
        assertTrue(lines.contains("PROBE filtered=899"), probe);
        assertTrue(lines.contains("PROBE groups=10"), probe);
        assertTrue(lines.stream().anyMatch(l -> l.startsWith("PROBE source=")
                && l.endsWith("/" + shaded.getFileName())), probe);
        // The renamed native library is found under the relocated prefix (Linux x86_64 / aarch64).
        String arch = System.getProperty("os.arch");
        if (System.getProperty("os.name").startsWith("Linux") && (arch.equals("amd64") || arch.equals("aarch64"))) {
            assertTrue(lines.contains("PROBE epoll=true"), probe);
        }
    }

    private static Path shadedJar() throws Exception {
        try (Stream<Path> jars = Files.list(Paths.get("target"))) {
            return jars.filter(p -> p.getFileName().toString().endsWith("-shaded.jar"))
                    .findFirst().orElseThrow(() -> new AssertionError("no target/*-shaded.jar: run after package"));
        }
    }

    private static String tail(List<String> lines) {
        return String.join("\n", lines.subList(Math.max(0, lines.size() - 60), lines.size()));
    }
}
