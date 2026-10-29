package pro.surpin.data.arrowflight.perf;

import pro.surpin.data.arrowflight.client.spark.FlightCatalog;

import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TPC-H through Spark local mode against a running Flight server, compared with Spark reading the same
 * Parquet files directly and with Spark's JDBC data source over the Flight SQL JDBC driver (baseline).
 *
 * <p>Run with {@code scripts/perf/tpch.sh} (generates the data, starts the server, runs this class) or by hand:
 * {@code ./mvnw -Pperf test -Dtest=TpchPerfTest -Dperf.tpch.data=<dir with tpch/<table>/*.parquet>}. Settings
 * (system properties): {@code perf.flight.host} / {@code perf.flight.port} (127.0.0.1:32010), {@code perf.flight.user} / {@code
 * perf.flight.password} (perf), {@code perf.flight.schema} (tpch), {@code perf.modes} (parquet,flight,jdbc; also
 * {@code catalog}: the tables of {@link FlightCatalog} instead of {@code format("flight")} views; parquet always
 * runs first as the reference), {@code perf.ansi} ({@code spark.sql.ansi.enabled} for every mode; unset: Spark's
 * default; Spark pushes aggregates over {@code + - * /} only in ANSI mode), {@code perf.runs} (3 timed runs per query and mode), {@code perf.queries} (all, or e.g. 1,6,14; {@code probes} or e.g. p1: probe scans, see {@link #queries}),
 * {@code perf.jdbc.partitions} (Spark JDBC partitions of the big tables, default the core count), {@code
 * perf.flight.options} (extra scan options of mode flight, see {@link #flightOptions}), {@code perf.out} (target/perf).
 *
 * <p>Writes {@code times.csv} (every run), {@code summary.md} (median per query and mode, totals, ratios),
 * {@code env.txt}, {@code plans/<mode>/qNN.txt} (executed plans and the task count of every stage of the first run) and {@code errors.txt}. Every mode's rows are
 * compared with the Parquet reference; a mismatch or failure fails the test after all queries ran.
 */
@Tag("perf")
class TpchPerfTest {

    private static final List<String> TABLES =
            List.of("lineitem", "orders", "customer", "part", "partsupp", "supplier", "nation", "region");
    /** Partition columns of the tables Spark JDBC splits into ranges (key column, upper bound per SF). */
    private static final Map<String, String> JDBC_KEYS = Map.of(
            "lineitem", "l_orderkey", "orders", "o_orderkey", "partsupp", "ps_partkey",
            "part", "p_partkey", "customer", "c_custkey");
    private static final int SORT_SCALE = 6;
    private static final BigDecimal ABSOLUTE_TOLERANCE = new BigDecimal("1e-6");
    private static final BigDecimal RELATIVE_TOLERANCE = new BigDecimal("1e-9");
    private static final String REFERENCE = "parquet";
    /** Spark catalog name of the {@code catalog} mode. */
    private static final String CATALOG = "flight";
    private static final String CATALOG_CONF = "spark.sql.catalog." + CATALOG;
    private static final String ANSI_CONF = "spark.sql.ansi.enabled";
    /**
     * Probe queries {@code p01..pNN} (not TPC-H): scans that isolate the read path, see the comments in the
     * resource files.
     */
    private static final int PROBES = 3;

    /**
     * Runs the selected TPC-H queries in every mode and writes the result files.
     *
     * @throws IOException when a query or result file cannot be read or written
     * @throws TimeoutException when Spark's listener bus does not drain after a query
     */
    @Test
    void tpch() throws IOException, TimeoutException {
        String data = System.getProperty("perf.tpch.data");
        Assumptions.assumeTrue(data != null && !data.isBlank(), "perf.tpch.data not set");
        Path tpch = Path.of(data).toAbsolutePath().resolve("tpch");
        Path out = Path.of(System.getProperty("perf.out", "target/perf")).toAbsolutePath();
        Files.createDirectories(out);
        int runs = Integer.parseInt(System.getProperty("perf.runs", "3"));
        List<String> modes = modes(System.getProperty("perf.modes", "parquet,flight,jdbc"));
        List<String> queries = queries(System.getProperty("perf.queries", "all"));
        int cores = Runtime.getRuntime().availableProcessors();

        SparkSession.Builder builder = SparkSession.builder()
                .appName("TpchPerfTest")
                .master("local[*]")
                .config("spark.sql.shuffle.partitions", String.valueOf(cores * 2))
                .config("spark.driver.bindAddress", "127.0.0.1")
                // Advertise the address bound: Spark 4 executors fetch session classes from the driver.
                .config("spark.driver.host", "127.0.0.1")
                .config("spark.ui.enabled", "false")
                .config(CATALOG_CONF, FlightCatalog.class.getName())
                .config(CATALOG_CONF + ".host", host())
                .config(CATALOG_CONF + ".port", port())
                .config(CATALOG_CONF + ".user", user())
                .config(CATALOG_CONF + ".password", password());
        String ansi = System.getProperty("perf.ansi", "");
        if (!ansi.isBlank()) {
            builder.config(ANSI_CONF, ansi.trim());
        }
        SparkSession spark = builder.getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        StageTasks stages = new StageTasks();
        spark.sparkContext().addSparkListener(stages);

        StringBuilder csv = new StringBuilder("mode,query,run,millis,rows\n");
        StringBuilder errors = new StringBuilder();
        Map<String, Map<String, long[]>> times = new LinkedHashMap<>();
        Map<String, List<List<Object>>> reference = new LinkedHashMap<>();
        long started = System.nanoTime();
        try {
            for (String mode : modes) {
                try {
                    register(spark, mode, tpch);
                } catch (RuntimeException e) {
                    errors.append(String.format(Locale.ROOT, "%s: tables not registered: %s%n", mode, e));
                    continue;
                }
                Path plans = Files.createDirectories(out.resolve("plans").resolve(mode));
                Map<String, long[]> modeTimes = new LinkedHashMap<>();
                times.put(mode, modeTimes);
                for (String q : queries) {
                    String sql = query(q);
                    long[] millis = new long[runs];
                    try {
                        List<List<Object>> rows = null;
                        for (int run = 0; run < runs; run++) {
                            stages.clear();
                            long t0 = System.nanoTime();
                            Dataset<Row> frame = spark.sql(sql);
                            List<Row> result = frame.collectAsList();
                            millis[run] = (System.nanoTime() - t0) / 1_000_000;
                            csv.append(String.format(Locale.ROOT, "%s,%s,%d,%d,%d%n",
                                    mode, q, run + 1, millis[run], result.size()));
                            if (run == 0) {
                                rows = canonical(result);
                                spark.sparkContext().listenerBus().waitUntilEmpty();
                                Files.writeString(plans.resolve(q + ".txt"),
                                        frame.queryExecution().executedPlan().toString() + "\n"
                                                + stageTasks(stages.tasks()) + "\n");
                            }
                        }
                        modeTimes.put(q, millis);
                        if (REFERENCE.equals(mode)) {
                            reference.put(q, rows);
                        } else if (reference.containsKey(q) && !sameRows(reference.get(q), rows)) {
                            errors.append(String.format(Locale.ROOT, "%s %s: rows differ from %s: %s%n",
                                    mode, q, REFERENCE, firstDifference(reference.get(q), rows)));
                        }
                    } catch (RuntimeException e) {
                        errors.append(String.format(Locale.ROOT, "%s %s: %s%n", mode, q, e));
                    }
                    System.out.printf(Locale.ROOT, "tpch %s %s %s ms%n", mode, q,
                            modeTimes.containsKey(q) ? Arrays.toString(millis) : "failed");
                }
            }
        } finally {
            Files.writeString(out.resolve("times.csv"), csv);
            Files.writeString(out.resolve("errors.txt"), errors);
            Files.writeString(out.resolve("summary.md"), summary(times, queries, runs));
            Files.writeString(out.resolve("env.txt"), String.join("\n",
                    "date=" + Instant.now(),
                    "spark=" + spark.version(),
                    "java=" + System.getProperty("java.version"),
                    "cores=" + cores,
                    "maxHeapMiB=" + Runtime.getRuntime().maxMemory() / (1024 * 1024),
                    "scaleFactor=" + System.getProperty("perf.sf", "unknown"),
                    "data=" + tpch,
                    "flight=" + host() + ":" + port() + " schema=" + schema(),
                    "flightOptions=" + System.getProperty("perf.flight.options", ""),
                    "modes=" + modes,
                    "ansi=" + spark.conf().get(ANSI_CONF),
                    "runs=" + runs,
                    "wallSeconds=" + (System.nanoTime() - started) / 1_000_000_000L,
                    ""));
            spark.stop();
        }
        assertTrue(errors.isEmpty(), errors.toString());
    }

    /**
     * Registers the TPC-H tables as temporary views read through the given mode.
     *
     * @param spark Spark session
     * @param mode parquet, flight, catalog or jdbc
     * @param tpch directory holding one Parquet directory per table
     */
    private static void register(SparkSession spark, String mode, Path tpch) {
        for (String table : TABLES) {
            Dataset<Row> frame = switch (mode) {
                case "parquet" -> spark.read().parquet(tpch.resolve(table).toString());
                case "flight" -> spark.read().format("flight")
                        .option("host", host())
                        .option("port", port())
                        .option("user", user())
                        .option("password", password())
                        .option("table", schema() + "." + table)
                        .options(flightOptions(System.getProperty("perf.flight.options", ""), table))
                        .load();
                case "catalog" -> spark.table(CATALOG + "." + schema() + "." + table);
                case "jdbc" -> jdbc(spark, table);
                default -> throw new IllegalArgumentException("unknown mode " + mode);
            };
            frame.createOrReplaceTempView(table);
        }
    }

    /**
     * Reads one table with Spark's JDBC data source and the Flight SQL JDBC driver; the big tables are split
     * into key ranges (one query per range) as a Spark JDBC job would.
     *
     * @param spark Spark session
     * @param table table name
     * @return the table's data frame
     */
    private static Dataset<Row> jdbc(SparkSession spark, String table) {
        String url = System.getProperty("perf.jdbc.url",
                "jdbc:arrow-flight-sql://" + host() + ":" + port() + "/?useEncryption=false&user=" + user()
                        + "&password=" + password());
        DataFrameReader reader = spark.read().format("jdbc")
                .option("url", url)
                .option("driver", "org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver")
                .option("dbtable", schema() + "." + table)
                .option("fetchsize", "10000");
        String key = JDBC_KEYS.get(table);
        if (key != null) {
            long upper = spark.read().format("jdbc").option("url", url)
                    .option("driver", "org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver")
                    .option("query", "select max(" + key + ") as m from " + schema() + "." + table)
                    .load().first().<Number>getAs(0).longValue();
            int partitions = Integer.parseInt(System.getProperty("perf.jdbc.partitions",
                    String.valueOf(Runtime.getRuntime().availableProcessors())));
            reader = reader.option("partitionColumn", key)
                    .option("lowerBound", "1")
                    .option("upperBound", String.valueOf(upper + 1))
                    .option("numPartitions", String.valueOf(partitions));
        }
        return reader.load();
    }

    /**
     * Rows in a comparable form: {@code float} values as {@code double}, integers as {@code long} (sources
     * differ in integer widths), rows sorted by a key in which non-integer numbers are rounded to 6 decimal
     * places (ties in ORDER BY may come in any order; Spark's {@code avg} of decimals has scale 6).
     *
     * @param rows query result
     * @return sorted rows of normalised values
     */
    static List<List<Object>> canonical(List<Row> rows) {
        List<List<Object>> result = new ArrayList<>(rows.size());
        for (Row row : rows) {
            List<Object> values = new ArrayList<>(row.length());
            for (int i = 0; i < row.length(); i++) {
                Object value = row.get(i);
                if (value instanceof Float f) {
                    value = f.doubleValue();
                } else if (value instanceof Integer || value instanceof Short || value instanceof Byte) {
                    value = ((Number) value).longValue();
                }
                values.add(value);
            }
            result.add(values);
        }
        result.sort(Comparator.comparing(TpchPerfTest::sortKey));
        return result;
    }

    /**
     * Sort key of a canonical row.
     *
     * @param row canonical row
     * @return key with numbers rounded to 6 decimal places
     */
    private static String sortKey(List<Object> row) {
        return row.stream().map(v -> numeric(v)
                ? decimal(v).setScale(SORT_SCALE, RoundingMode.HALF_UP).stripTrailingZeros().toPlainString()
                : String.valueOf(v))
                .collect(Collectors.joining("|"));
    }

    /**
     * Compares canonical rows: numbers by value (a source may type an integer column as {@code decimal(20,0)});
     * decimals and doubles may differ by 1e-6 (Spark rounds {@code avg} of decimals
     * to scale 6, other sources keep more digits); when a double is involved also by 1e-9 relative (sums added
     * in another order); everything else, exact decimal sums included, must be equal.
     *
     * @param expected reference rows
     * @param actual rows to check
     * @return whether they match
     */
    static boolean sameRows(List<List<Object>> expected, List<List<Object>> actual) {
        if (expected.size() != actual.size()) {
            return false;
        }
        for (int r = 0; r < expected.size(); r++) {
            List<Object> e = expected.get(r);
            List<Object> a = actual.get(r);
            if (e.size() != a.size()) {
                return false;
            }
            for (int c = 0; c < e.size(); c++) {
                if (!sameValue(e.get(c), a.get(c))) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Compares two values as {@link #sameRows} does.
     *
     * @param expected reference value
     * @param actual value to check
     * @return whether they match
     */
    private static boolean sameValue(Object expected, Object actual) {
        if (numeric(expected) && numeric(actual)) {
            BigDecimal e = decimal(expected);
            BigDecimal tolerance;
            if (expected instanceof Long || actual instanceof Long) {
                tolerance = BigDecimal.ZERO;
            } else if (expected instanceof Double || actual instanceof Double) {
                tolerance = e.abs().multiply(RELATIVE_TOLERANCE).max(ABSOLUTE_TOLERANCE);
            } else {
                tolerance = ABSOLUTE_TOLERANCE;
            }
            return e.subtract(decimal(actual)).abs().compareTo(tolerance) <= 0;
        }
        return Objects.equals(expected, actual);
    }

    /**
     * A decimal, double or long as an exact decimal.
     *
     * @param value {@link BigDecimal}, {@link Double} or {@link Long}
     * @return the decimal value
     */
    private static BigDecimal decimal(Object value) {
        if (value instanceof Long l) {
            return BigDecimal.valueOf(l);
        }
        return value instanceof BigDecimal d ? d : new BigDecimal((Double) value);
    }

    /**
     * Whether a canonical value is compared as a number.
     *
     * @param value canonical value
     * @return true for long, double and decimal values
     */
    private static boolean numeric(Object value) {
        return value instanceof Long || value instanceof Double || value instanceof BigDecimal;
    }

    /**
     * Markdown summary: median milliseconds per query and mode, totals and ratios to the reference.
     *
     * @param times timings per mode and query
     * @param queries query names
     * @param runs runs per query
     * @return markdown table
     */
    static String summary(Map<String, Map<String, long[]>> times, List<String> queries, int runs) {
        List<String> modes = new ArrayList<>(times.keySet());
        StringBuilder md = new StringBuilder("TPC-H, median of " + runs + " runs (ms)\n\n| query |");
        for (String mode : modes) {
            md.append(' ').append(mode).append(" |");
        }
        for (String mode : modes) {
            if (!REFERENCE.equals(mode) && times.containsKey(REFERENCE)) {
                md.append(' ').append(mode).append('/').append(REFERENCE).append(" |");
            }
        }
        md.append('\n').append("|---".repeat(1 + modes.size() * 2 - (times.containsKey(REFERENCE) ? 1 : 0)))
                .append("|\n");
        Map<String, Long> totals = new LinkedHashMap<>();
        for (String q : queries) {
            md.append("| ").append(q).append(" |");
            for (String mode : modes) {
                long[] t = times.get(mode).get(q);
                md.append(' ').append(t == null ? "failed" : String.valueOf(median(t))).append(" |");
                if (t != null) {
                    totals.merge(mode, median(t), Long::sum);
                }
            }
            appendRatios(md, times, modes, q);
            md.append('\n');
        }
        md.append("| total |");
        for (String mode : modes) {
            md.append(' ').append(totals.getOrDefault(mode, 0L)).append(" |");
        }
        for (String mode : modes) {
            if (!REFERENCE.equals(mode) && totals.containsKey(REFERENCE)) {
                md.append(String.format(Locale.ROOT, " %.2f |",
                        (double) totals.getOrDefault(mode, 0L) / totals.get(REFERENCE)));
            }
        }
        return md.append("\n\nTotals add the medians of the queries that ran in that mode.\n").toString();
    }

    /**
     * Appends each mode's ratio to the reference for one query.
     *
     * @param md markdown under construction
     * @param times timings
     * @param modes modes in order
     * @param q query name
     */
    private static void appendRatios(StringBuilder md, Map<String, Map<String, long[]>> times,
            List<String> modes, String q) {
        long[] ref = times.containsKey(REFERENCE) ? times.get(REFERENCE).get(q) : null;
        for (String mode : modes) {
            if (REFERENCE.equals(mode) || !times.containsKey(REFERENCE)) {
                continue;
            }
            long[] t = times.get(mode).get(q);
            md.append(ref == null || t == null ? " - |"
                    : String.format(Locale.ROOT, " %.2f |", (double) median(t) / Math.max(1, median(ref))));
        }
    }

    /**
     * Median of the runs (upper median for an even count).
     *
     * @param values run times
     * @return median
     */
    static long median(long[] values) {
        long[] sorted = values.clone();
        Arrays.sort(sorted);
        return sorted[sorted.length / 2];
    }

    /**
     * Parses the mode list; the reference mode always runs first.
     *
     * @param value comma-separated modes
     * @return modes in run order
     */
    static List<String> modes(String value) {
        List<String> modes = new ArrayList<>(List.of(REFERENCE));
        Arrays.stream(value.split(",")).map(String::trim).filter(m -> !m.isEmpty() && !modes.contains(m))
                .forEach(modes::add);
        return modes;
    }

    /**
     * Parses extra scan options of mode {@code flight}: {@code table:key=value} entries separated by {@code ;},
     * table {@code *} for every table; e.g. {@code lineitem:partition.byColumn=l_orderkey;lineitem:partition.size=4}.
     *
     * @param value option list (empty: none)
     * @param table table being registered
     * @return the options for that table, in order
     */
    static Map<String, String> flightOptions(String value, String table) {
        Map<String, String> options = new LinkedHashMap<>();
        for (String entry : value.split(";")) {
            String e = entry.trim();
            if (e.isEmpty()) {
                continue;
            }
            int colon = e.indexOf(':');
            int equals = e.indexOf('=');
            if (colon <= 0 || equals < colon + 2) {
                throw new IllegalArgumentException("perf.flight.options entry is not table:key=value: " + e);
            }
            String target = e.substring(0, colon).trim();
            if ("*".equals(target) || target.equalsIgnoreCase(table)) {
                options.put(e.substring(colon + 1, equals).trim(), e.substring(equals + 1).trim());
            }
        }
        return options;
    }

    /**
     * Parses the query selection: {@code all} (the 22 TPC-H queries), {@code probes} (every {@link #PROBES}
     * query), query numbers ({@code 5} = {@code q05}) and probe names ({@code p1} = {@code p01}), comma-separated.
     *
     * @param value selection
     * @return query names ({@code qNN}, {@code pNN}) in order
     */
    static List<String> queries(String value) {
        List<String> names = new ArrayList<>();
        for (String token : value.split(",")) {
            String t = token.trim().toLowerCase(Locale.ROOT);
            if (t.isEmpty()) {
                continue;
            }
            if ("all".equals(t)) {
                for (int q = 1; q <= 22; q++) {
                    names.add(String.format(Locale.ROOT, "q%02d", q));
                }
            } else if ("probes".equals(t)) {
                for (int p = 1; p <= PROBES; p++) {
                    names.add(String.format(Locale.ROOT, "p%02d", p));
                }
            } else if (t.startsWith("p") || t.startsWith("q")) {
                names.add(String.format(Locale.ROOT, "%c%02d", t.charAt(0), Integer.parseInt(t.substring(1))));
            } else {
                names.add(String.format(Locale.ROOT, "q%02d", Integer.parseInt(t)));
            }
        }
        return names;
    }

    /**
     * Loads a query text from the test resources.
     *
     * @param name query name ({@code qNN} TPC-H, {@code pNN} probe)
     * @return SQL text
     * @throws IOException when the resource cannot be read
     */
    private static String query(String name) throws IOException {
        String resource = "/tpch-queries/" + name + ".sql";
        try (InputStream in = Objects.requireNonNull(TpchPerfTest.class.getResourceAsStream(resource), resource)) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    /**
     * Describes the first row where two results differ, with the values' classes.
     *
     * @param expected reference rows
     * @param actual rows that differ
     * @return description for the error report
     */
    static String firstDifference(List<List<Object>> expected, List<List<Object>> actual) {
        if (expected.size() != actual.size()) {
            return expected.size() + " rows expected, " + actual.size() + " returned";
        }
        for (int r = 0; r < expected.size(); r++) {
            if (!sameRows(List.of(expected.get(r)), List.of(actual.get(r)))) {
                return "row " + r + ": expected " + typed(expected.get(r)) + ", got " + typed(actual.get(r));
            }
        }
        return "no difference";
    }

    /**
     * A row with each value's class.
     *
     * @param row canonical row
     * @return printable row
     */
    private static String typed(List<Object> row) {
        return row.stream().map(v -> v == null ? "null" : v + " (" + v.getClass().getSimpleName() + ")")
                .collect(Collectors.joining(", ", "[", "]"));
    }

    /**
     * Flight server host.
     *
     * @return host name
     */
    private static String host() {
        return System.getProperty("perf.flight.host", "127.0.0.1");
    }

    /**
     * Flight server port.
     *
     * @return port
     */
    private static String port() {
        return System.getProperty("perf.flight.port", "32010");
    }

    /**
     * User name sent to the server (any value for a server without authentication).
     *
     * @return user name
     */
    private static String user() {
        return System.getProperty("perf.flight.user", "perf");
    }

    /**
     * Password sent to the server.
     *
     * @return password
     */
    private static String password() {
        return System.getProperty("perf.flight.password", "perf");
    }

    /**
     * Server schema holding the TPC-H tables.
     *
     * @return schema name
     */
    private static String schema() {
        return System.getProperty("perf.flight.schema", "tpch");
    }

    /**
     * One line with the task count of each completed stage, in stage order: for a scan stage the number of
     * input partitions (Flight endpoints, Parquet splits), for a stage after a shuffle its partitions.
     *
     * @param tasks task count per stage id
     * @return e.g. {@code stage tasks (id:tasks): 3:4 4:8}
     */
    static String stageTasks(SortedMap<Integer, Integer> tasks) {
        return tasks.entrySet().stream().map(e -> e.getKey() + ":" + e.getValue())
                .collect(Collectors.joining(" ", "stage tasks (id:tasks): ", ""));
    }

    /** Records the task count of every completed stage (reset before each run). */
    private static final class StageTasks extends SparkListener {

        private final SortedMap<Integer, Integer> tasks = new TreeMap<>();

        @Override
        public synchronized void onStageCompleted(SparkListenerStageCompleted completed) {
            tasks.put(completed.stageInfo().stageId(), completed.stageInfo().numTasks());
        }

        /** Forgets the stages recorded so far. */
        synchronized void clear() {
            tasks.clear();
        }

        /**
         * The stages recorded since the last {@link #clear()}.
         *
         * @return task count per stage id
         */
        synchronized SortedMap<Integer, Integer> tasks() {
            return new TreeMap<>(tasks);
        }
    }
}
