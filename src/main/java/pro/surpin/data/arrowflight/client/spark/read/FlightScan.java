package pro.surpin.data.arrowflight.client.spark.read;

import pro.surpin.data.arrowflight.client.Configuration;
import pro.surpin.data.arrowflight.client.expr.SqlRenderer;
import pro.surpin.data.arrowflight.client.model.Table;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.SupportsReportStatistics;
import org.apache.spark.sql.connector.read.SupportsRuntimeFiltering;
import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.sources.LessThanOrEqual;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalLong;

/**
 * Describes the data-structure of FlightScan.
 *
 * <p>When Spark keeps the pushed filters of a join's other side in its plan
 * ({@code spark.flight.retainPushedFilters}), Spark's dynamic pruning may narrow this scan at run time to the join
 * keys it found there: {@link #filter(Filter[])} plans the scan again on the server with an extra WHERE condition.
 * Spark still performs the join, so the server only has to return a superset of the matching rows.
 *
 * <p>Conditions Spark cannot translate into connector predicates but the server evaluates exactly (multi-wildcard
 * {@code LIKE} patterns, see {@code FlightLikePushdown}) are added after Spark's push-down with
 * {@link #pushConditions(List)}; Spark keeps evaluating them as well.
 */
public class FlightScan implements SupportsReportStatistics, SupportsRuntimeFiltering, Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlightScan.class);

    /** Spark SQL setting: smallest table (server row estimate) offered for runtime filtering. */
    public static final String RUNTIME_FILTER_MIN_ROWS = "spark.flight.runtimeFilterMinRows";
    /** Spark SQL setting: from this many keys an integer runtime filter becomes a min/max range. */
    public static final String RUNTIME_FILTER_RANGE_MIN_KEYS = "spark.flight.runtimeFilterRangeMinKeys";
    static final long DEFAULT_RUNTIME_FILTER_MIN_ROWS = 50_000_000L;
    static final int DEFAULT_RUNTIME_FILTER_RANGE_MIN_KEYS = 1_000;
    // Longest IN list sent to the server; longer key lists that cannot become a range are dropped (no pruning).
    static final int MAX_RUNTIME_KEYS = 100_000;

    private final Configuration configuration;
    private final Table table;
    // Plans the scan again with extra WHERE conditions; null for an aggregated scan. Driver-side only.
    private final transient Replanner replanner;
    // True when Spark's dynamic pruning may narrow the scan (spark.flight.retainPushedFilters is not false).
    private final boolean runtimeFiltering;
    // True when the pushed predicates were also returned to Spark, which lets Spark prune other scans with them.
    private final boolean retained;
    // True when the scan returns partial aggregates (an aggregation was pushed) instead of rows.
    private final boolean aggregated;
    // WHERE conditions applied by the last runtime filtering, for the plan description and tests.
    private List<String> runtimeConditions = List.of();
    // WHERE conditions added after Spark's push-down (pushConditions), kept when runtime filters re-plan the scan.
    private List<String> pushedConditions = List.of();

    /** Plans the scan again on the Flight server with extra filter conditions. */
    @FunctionalInterface
    public interface Replanner {
        /**
         * Applies the conditions on top of the filters the scan already carries.
         *
         * @param extraConditions rendered WHERE conditions
         */
        void replan(List<String> extraConditions);
    }

    /**
     * Construct a FligthScan
     * @param configuration - the configuration of remote flight service
     * @param table - the table object
     */
    public FlightScan(Configuration configuration, Table table) {
        this(configuration, table, null, false);
    }

    /**
     * Construct a FlightScan that may accept runtime filters.
     * @param configuration - the configuration of remote flight service
     * @param table - the table object
     * @param replanner - plans the scan again with extra conditions, or null to refuse runtime filters
     * @param retained - true when the pushed predicates were returned to Spark as well
     */
    public FlightScan(Configuration configuration, Table table, Replanner replanner, boolean retained) {
        this(configuration, table, replanner, retained, false);
    }

    /**
     * Construct a FlightScan that may accept runtime filters and may return partial aggregates.
     * @param configuration - the configuration of remote flight service
     * @param table - the table object
     * @param replanner - plans the scan again with extra conditions, or null to refuse runtime filters
     * @param retained - true when the pushed predicates were returned to Spark as well
     * @param aggregated - true when an aggregation was pushed into the scan
     */
    public FlightScan(Configuration configuration, Table table, Replanner replanner, boolean retained,
            boolean aggregated) {
        this(configuration, table, replanner, retained, aggregated, replanner != null);
    }

    /**
     * Construct a FlightScan.
     * @param configuration - the configuration of remote flight service
     * @param table - the table object
     * @param replanner - plans the scan again with extra conditions, or null when it cannot be re-planned
     * @param retained - true when the pushed predicates were returned to Spark as well
     * @param aggregated - true when an aggregation was pushed into the scan
     * @param runtimeFiltering - true to offer the scan to Spark's dynamic pruning (needs a replanner)
     */
    public FlightScan(Configuration configuration, Table table, Replanner replanner, boolean retained,
            boolean aggregated, boolean runtimeFiltering) {
        LOGGER.debug("{}()", this.getClass().getName());

        this.configuration = configuration;
        this.table = table;
        this.replanner = replanner;
        this.retained = retained;
        this.aggregated = aggregated;
        this.runtimeFiltering = runtimeFiltering && replanner != null;
    }

    /**
     * Tells whether the scan returns partial aggregates of a pushed aggregation instead of rows.
     *
     * @return true when an aggregation was pushed
     */
    public boolean aggregated() {
        return this.aggregated;
    }

    /**
     * Tells whether Spark keeps this scan's pushed predicates in its plan.
     *
     * @return true when the predicates were retained
     */
    public boolean retainsFilters() {
        return this.retained;
    }

    /**
     * Names the scanned table or query.
     *
     * @return the source, possibly null
     */
    public String tableName() {
        return this.table.getName();
    }

    /**
     * Returns the WHERE conditions the last runtime filtering added.
     *
     * @return rendered conditions, empty when none applied
     */
    public List<String> runtimeConditions() {
        return this.runtimeConditions;
    }

    /**
     * Returns the WHERE conditions added after Spark's push-down.
     *
     * @return rendered conditions, empty when none
     */
    public synchronized List<String> pushedConditions() {
        return this.pushedConditions;
    }

    /**
     * Returns the SQL renderer of the scanned table (its identifier quoting).
     *
     * @return the renderer
     */
    public SqlRenderer renderer() {
        return this.table.renderer();
    }

    /**
     * Adds WHERE conditions the server evaluates exactly and plans the scan again with them. The caller keeps
     * evaluating the conditions in Spark, so the scan's rows stay correct whichever plan reads them. Conditions the
     * scan already carries are skipped; an aggregated scan cannot take conditions.
     *
     * @param conditions rendered WHERE conditions over the scan's columns
     * @return true when the scan was planned again
     */
    public synchronized boolean pushConditions(List<String> conditions) {
        if (this.replanner == null || this.aggregated) {
            return false;
        }
        List<String> all = new ArrayList<>(this.pushedConditions);
        conditions.stream().filter(c -> !all.contains(c)).distinct().forEach(all::add);
        if (all.size() == this.pushedConditions.size()) {
            return false;
        }
        List<String> replan = new ArrayList<>(all);
        replan.addAll(this.runtimeConditions);
        this.replanner.replan(replan);
        this.pushedConditions = List.copyOf(all);
        return true;
    }

    /**
     * The integer columns Spark may prune with the join keys of a filtered other side; none when runtime filtering
     * is off or the table is smaller than {@code spark.flight.runtimeFilterMinRows}.
     *
     * @return filterable columns
     */
    @Override
    public NamedReference[] filterAttributes() {
        StructType schema = readSchema();
        if (!this.runtimeFiltering || schema == null || tooSmallToPrune()) {
            return new NamedReference[0];
        }
        return Arrays.stream(schema.fields())
                .filter(f -> isKey(f.dataType()))
                .map(f -> Expressions.column(f.name()))
                .toArray(NamedReference[]::new);
    }

    private boolean tooSmallToPrune() {
        long rows = this.table.getEstimatedRows();
        long minimum = FlightScanBuilder.sparkSetting(RUNTIME_FILTER_MIN_ROWS, DEFAULT_RUNTIME_FILTER_MIN_ROWS);
        return rows >= 0 && rows < minimum;
    }

    private static boolean isKey(DataType type) {
        return DataTypes.LongType.equals(type) || DataTypes.IntegerType.equals(type);
    }

    /**
     * Narrows the scan to the join keys Spark found at run time and plans it again on the server. Up to
     * {@code spark.flight.runtimeFilterRangeMinKeys} keys go to the server as an IN condition, more as a range of
     * their minimum and maximum (lets the server skip row groups without a huge statement); more than
     * {@value #MAX_RUNTIME_KEYS} keys that cannot form a range are not sent.
     *
     * @param filters IN filters on the columns of {@link #filterAttributes()}
     */
    @Override
    public synchronized void filter(Filter[] filters) {
        if (!this.runtimeFiltering) {
            return;
        }
        List<String> conditions = new ArrayList<>();
        long rangeMinKeys = FlightScanBuilder.sparkSetting(RUNTIME_FILTER_RANGE_MIN_KEYS,
                DEFAULT_RUNTIME_FILTER_RANGE_MIN_KEYS);
        for (Filter filter : filters) {
            Filter runtime = runtimeCondition(filter, rangeMinKeys);
            if (runtime != null && this.table.canPushFilter(runtime)) {
                conditions.add(this.table.toWhereClause(runtime));
            }
        }
        if (!conditions.isEmpty()) {
            LOGGER.debug("{}.filter(): {} runtime conditions", this.getClass().getName(), conditions.size());
            List<String> replan = new ArrayList<>(this.pushedConditions);
            replan.addAll(conditions);
            this.replanner.replan(replan);
            this.runtimeConditions = List.copyOf(conditions);
        }
    }

    /**
     * Turns one runtime filter into the condition sent to the server.
     *
     * @param filter runtime filter from Spark
     * @param rangeMinKeys smallest key count sent as a min/max range
     * @return the condition, or null to skip the filter
     */
    static Filter runtimeCondition(Filter filter, long rangeMinKeys) {
        if (!(filter instanceof In in) || in.values().length == 0) {
            return null;
        }
        Object[] values = in.values();
        if (values.length >= rangeMinKeys) {
            Filter range = range(in.attribute(), values);
            if (range != null) {
                return range;
            }
        }
        return values.length <= MAX_RUNTIME_KEYS ? in : null;
    }

    private static Filter range(String attribute, Object[] values) {
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        boolean ints = true;
        for (Object value : values) {
            if (value == null) {
                continue; // a null key never matches a join
            }
            if (!(value instanceof Integer || value instanceof Long)) {
                return null;
            }
            ints &= value instanceof Integer;
            long key = ((Number) value).longValue();
            min = Math.min(min, key);
            max = Math.max(max, key);
        }
        if (min > max) {
            return null;
        }
        Object low = ints ? (Object) Integer.valueOf((int) min) : (Object) Long.valueOf(min);
        Object high = ints ? (Object) Integer.valueOf((int) max) : (Object) Long.valueOf(max);
        return new And(new GreaterThanOrEqual(attribute, low), new LessThanOrEqual(attribute, high));
    }

    /**
     * Get the schema of the scan
     * @return - the scan for the scan
     */
    @Override
    public StructType readSchema() {
        return this.table.getSparkSchema();
    }

    /**
     * The description of the scan
     * @return - description
     */
    @Override
    public String description() {
        return this.table.getQueryStatement();
    }

    /**
     * Reports Parquet input estimates transported in the standard FlightInfo fields.
     *
     * @return Spark scan statistics
     */
    @Override
    public Statistics estimateStatistics() {
        long bytes = this.table.getEstimatedBytes();
        long rows = this.table.getEstimatedRows();
        return new Statistics() {
            @Override
            public OptionalLong sizeInBytes() {
                return bytes >= 0 ? OptionalLong.of(bytes) : OptionalLong.empty();
            }

            @Override
            public OptionalLong numRows() {
                return rows >= 0 ? OptionalLong.of(rows) : OptionalLong.empty();
            }
        };
    }

    /**
     * Translate the scan to batch
     * @return - the batch desribes the scan
     */
    @Override
    public Batch toBatch() {
        LOGGER.debug("{}.toBatch()", this.getClass().getName());
        return new FlightBatch(this.configuration, this.table);
    }
}
