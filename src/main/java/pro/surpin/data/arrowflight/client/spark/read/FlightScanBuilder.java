package pro.surpin.data.arrowflight.client.spark.read;

import pro.surpin.data.arrowflight.client.Configuration;
import pro.surpin.data.arrowflight.client.write.PartitionBehavior;
import pro.surpin.data.arrowflight.client.query.PushAggregation;
import pro.surpin.data.arrowflight.client.model.Table;
import pro.surpin.data.arrowflight.client.spark.SparkPushdown;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownAggregates;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.connector.read.SupportsPushDownV2Filters;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Build flight scans which supports pushed-down filter, fields & aggregates
 */
public final class FlightScanBuilder implements ScanBuilder, SupportsPushDownV2Filters,
        SupportsPushDownRequiredColumns, SupportsPushDownAggregates {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlightScanBuilder.class);

    /**
     * Spark SQL setting: {@code false} (default), {@code true} or {@code auto}. When on, accepted predicates are
     * returned to Spark as well (the server still applies them), so Spark's dynamic pruning sees a selective filter
     * and may narrow a join's other Flight scan at run time. Kept filters stop Spark from pushing aggregations.
     */
    public static final String RETAIN_PUSHED_FILTERS = "spark.flight.retainPushedFilters";
    /** Spark SQL setting: in {@code auto} mode, the largest table (server rows) whose filters are kept. */
    public static final String RETAIN_MAX_ROWS = "spark.flight.retainMaxRows";
    /** Spark SQL setting: in {@code auto} mode, the largest estimated share of rows the filters may keep. */
    public static final String RETAIN_MAX_SELECTIVITY = "spark.flight.retainMaxSelectivity";
    static final long DEFAULT_RETAIN_MAX_ROWS = 50_000_000L;
    static final double DEFAULT_RETAIN_MAX_SELECTIVITY = 0.1;

    private final Configuration configuration;
    private final Table table;
    private final PartitionBehavior partitionBehavior;
    //whether the reported scan size is scaled by the estimated filter selectivity
    private final boolean filterSelectivityEstimate;

    //the pushed-down filters
    private Filter[] pdFilters = new Filter[0];
    //the pushed-down V2 predicates
    private Predicate[] pdPredicates = new Predicate[0];
    // true when the pushed predicates were returned to Spark as well
    private boolean retained;
    //the pushed-down columns
    private StructField[] pdColumns = new StructField[0];

    //pushed-down aggregation
    private PushAggregation pdAggregation = null;

    /**
     * Construct a flight-scan builder
     * @param configuration - the configuration of remote flight-service
     * @param table - the table instance representing a table in remote flight-service
     * @param partitionBehavior - the partitioning behavior when loading data from remote flight service
     */
    public FlightScanBuilder(Configuration configuration, Table table, PartitionBehavior partitionBehavior) {
        this(configuration, table, partitionBehavior, false);
    }

    /**
     * Construct a flight-scan builder
     * @param configuration - the configuration of remote flight-service
     * @param table - the table instance representing a table in remote flight-service
     * @param partitionBehavior - the partitioning behavior when loading data from remote flight service
     * @param filterSelectivityEstimate - scale the reported scan size by the estimated selectivity of pushed filters
     */
    public FlightScanBuilder(Configuration configuration, Table table, PartitionBehavior partitionBehavior,
            boolean filterSelectivityEstimate) {
        LOGGER.debug("{}()", this.getClass().getName());
        this.configuration = configuration;
        this.table = table;
        this.partitionBehavior = partitionBehavior;
        this.filterSelectivityEstimate = filterSelectivityEstimate;
    }

    /**
     * Collection aggregations that will be pushed down
     * @param aggregation - the pushed aggregation
     * @return - pushed aggregation
     */
    @Override
    public boolean pushAggregation(Aggregation aggregation) {
        LOGGER.debug("{}.pushAggregation()", this.getClass().getName());

        // An aggregation with neither aggregate functions nor grouping keys has
        // nothing to push; SparkPushdown declines it so Spark keeps the plain scan.
        this.pdAggregation = SparkPushdown.translate(aggregation, this.table.getSparkSchema())
                .map(this.table.renderer()::render)
                .orElse(null);
        return this.pdAggregation != null;
    }

    /**
     * For SupportsPushDownFilters interface
     * @param filters - the pushed-down filters
     * @return - not-accepted filters
     */
    public Filter[] pushFilters(Filter[] filters) {
        LOGGER.debug("{}.pushFilters()", this.getClass().getName());

        List<Filter> pushed = new ArrayList<>();
        List<Filter> unhandled = new ArrayList<>();
        for (Filter filter : filters) {
            if (this.table.canPushFilter(filter)) {
                pushed.add(filter);
            } else {
                unhandled.add(filter);
            }
        }
        this.pdFilters = pushed.toArray(new Filter[0]);
        return unhandled.toArray(new Filter[0]);
    }

    /**
     * For SupportsPushDownFilters interface
     * @return - the pushed-down filters
     */
    public Filter[] pushedFilters() {
        return this.pdFilters;
    }

    /**
     * Accepts Spark V2 predicates that the Flight SQL server can evaluate exactly.
     *
     * @param predicates candidate predicates
     * @return predicates that still need evaluation in Spark
     */
    @Override
    public Predicate[] pushPredicates(Predicate[] predicates) {
        LOGGER.debug("{}.pushPredicates()", this.getClass().getName());

        List<Predicate> pushed = new ArrayList<>();
        List<Predicate> unhandled = new ArrayList<>();
        for (Predicate predicate : predicates) {
            if (this.table.canPushPredicate(predicate)) {
                pushed.add(predicate);
            } else {
                unhandled.add(predicate);
            }
        }
        this.pdPredicates = pushed.toArray(new Predicate[0]);
        this.retained = !pushed.isEmpty() && retainsFilters();
        if (this.retained) {
            unhandled.addAll(pushed);
        }
        return unhandled.toArray(new Predicate[0]);
    }

    /**
     * Decides whether the accepted predicates stay in Spark's plan ({@value #RETAIN_PUSHED_FILTERS}); in
     * {@code auto} mode from the server's statistics of a plain table.
     *
     * @return true to return the pushed predicates to Spark as well
     */
    private boolean retainsFilters() {
        String mode = sparkSetting(RETAIN_PUSHED_FILTERS, "false");
        if (!"auto".equalsIgnoreCase(mode)) {
            return shouldRetain(mode, -1, 1.0, 0, 0.0);
        }
        if (!ServerColumnStatistics.describable(this.table.getName())) {
            return false;
        }
        ServerColumnStatistics.Answer stats = ServerColumnStatistics.fetch(this.configuration, this.table.getName(),
                ScanSelectivity.columns(this.pdPredicates));
        return shouldRetain(mode, stats.rows(), ScanSelectivity.of(this.pdPredicates, stats),
                sparkSetting(RETAIN_MAX_ROWS, DEFAULT_RETAIN_MAX_ROWS),
                sparkSetting(RETAIN_MAX_SELECTIVITY, DEFAULT_RETAIN_MAX_SELECTIVITY));
    }

    /**
     * Decides whether pushed filters stay in Spark's plan. Dynamic pruning collects the join keys of the filtered
     * side, so in {@code auto} mode only a table of known, bounded size cut down to a small share qualifies.
     *
     * @param mode {@code true}, {@code false} or {@code auto}
     * @param rows rows in the table, negative when unknown
     * @param selectivity estimated share of the rows the filters keep
     * @param maxRows largest table kept in {@code auto} mode
     * @param maxSelectivity largest kept share in {@code auto} mode
     * @return true to return the pushed predicates to Spark as well
     */
    static boolean shouldRetain(String mode, long rows, double selectivity, long maxRows, double maxSelectivity) {
        if ("true".equalsIgnoreCase(mode)) {
            return true;
        }
        return "auto".equalsIgnoreCase(mode) && rows >= 0 && rows <= maxRows && selectivity <= maxSelectivity;
    }

    /**
     * Reads a Spark SQL setting of the active session.
     *
     * @param key setting name
     * @param fallback value without a setting or an active session
     * @return the setting
     */
    static String sparkSetting(String key, String fallback) {
        // Not SparkSession.active(): without a session Spark 4 throws a checked SparkException from it.
        scala.Option<org.apache.spark.sql.SparkSession> session = org.apache.spark.sql.SparkSession.getActiveSession();
        if (session.isEmpty()) {
            session = org.apache.spark.sql.SparkSession.getDefaultSession();
        }
        return session.isEmpty() ? fallback : session.get().conf().get(key, fallback);
    }

    /**
     * Reads a numeric Spark SQL setting of the active session.
     *
     * @param key setting name
     * @param fallback value without a (valid) setting
     * @return the setting
     */
    static long sparkSetting(String key, long fallback) {
        try {
            return Long.parseLong(sparkSetting(key, Long.toString(fallback)).trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(key + " must be a whole number", e);
        }
    }

    /**
     * Reads a fractional Spark SQL setting of the active session.
     *
     * @param key setting name
     * @param fallback value without a setting
     * @return the setting
     */
    static double sparkSetting(String key, double fallback) {
        try {
            return Double.parseDouble(sparkSetting(key, Double.toString(fallback)).trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(key + " must be a number", e);
        }
    }

    /**
     * Returns the Spark V2 predicates accepted by this scan.
     *
     * @return pushed V2 predicates
     */
    @Override
    public Predicate[] pushedPredicates() {
        return this.pdPredicates;
    }

    /**
     * For SupportsPushDownRequiredColumns interface
     * @param columns - the schema containing the required columns
     */
    @Override
    public void pruneColumns(StructType columns) {
        this.pdColumns = columns.fields();
        LOGGER.debug("{}.pruneColumns(): columns={}", this.getClass().getName(),
                Arrays.toString(Arrays.stream(this.pdColumns)
                        .map(StructField::name).toArray(String[]::new)));
    }

    /**
     * To build a flight-scan
     * @return - A flight scan
     */
    @Override
    public Scan build() {
        LOGGER.debug("{}.build()", this.getClass().getName());

        //adjust flight-table upon pushed filters & columns
        List<String> whereParts = new ArrayList<>();
        Arrays.stream(this.pdFilters).map(this.table::toWhereClause)
                .forEach(whereParts::add);
        Arrays.stream(this.pdPredicates).map(this.table::toWhereClause)
                .forEach(whereParts::add);
        String where = String.join(" and ", whereParts);
        LOGGER.debug("{}.build(): columns={} filters={} predicates={}",
                this.getClass().getName(),
                Arrays.toString(Arrays.stream(this.pdColumns)
                        .map(StructField::name).toArray(String[]::new)),
                this.pdFilters.length, this.pdPredicates.length);
        this.table.probe(where, this.pdColumns, this.pdAggregation, this.partitionBehavior);
        // The server estimates the raw input; a global aggregate yields one row, and with the
        // opt-in estimate the pushed filters shrink it so Spark can broadcast filtered tables.
        this.table.setPushdownEstimate(
                this.filterSelectivityEstimate ? filterSelectivity() : 1.0,
                isGlobalAggregation(this.pdAggregation));
        // Plan exactly once, after Spark has supplied every accepted pushdown.
        // Earlier schema-only planning creates unused server handles and makes
        // Flight latency look worse than the actual scan.
        this.table.initialize(this.configuration);
        // Runtime filtering re-plans the scan with the same pushdown plus the conditions Spark found at run time.
        StructField[] columns = this.pdColumns;
        PushAggregation aggregation = this.pdAggregation;
        PartitionBehavior behavior = this.partitionBehavior;
        Configuration config = this.configuration;
        Table scanTable = this.table;
        // Off by default: with retainPushedFilters=false the scan offers nothing to Spark's dynamic pruning either.
        // The replanner also takes the conditions added after Spark's push-down (FlightLikePushdown).
        boolean runtimeFiltering = !"false".equalsIgnoreCase(sparkSetting(RETAIN_PUSHED_FILTERS, "false").trim());
        FlightScan.Replanner replanner = aggregation != null ? null : new FlightScan.Replanner() {
            @Override
            public void replan(List<String> extra) {
                replan(extra, null);
            }

            @Override
            public boolean replan(List<String> extra, List<String> names) {
                StructField[] read = columns;
                if (names != null) {
                    // Only a subset of the pushed columns, in their order; with no pushed columns nothing is pruned.
                    read = Arrays.stream(columns).filter(f -> names.contains(f.name())).toArray(StructField[]::new);
                    if (columns.length == 0 || read.length != names.size()) {
                        return false;
                    }
                }
                List<String> all = new ArrayList<>(whereParts);
                extra.forEach(condition -> all.add("(" + condition + ")"));
                scanTable.probe(String.join(" and ", all), read, null, behavior);
                scanTable.initialize(config);
                return true;
            }
        };
        return new FlightScan(this.configuration, this.table, replanner, this.retained, aggregation != null,
                runtimeFiltering);
    }

    /**
     * Estimates the share of rows the pushed predicates keep, refined by the server's column statistics of the
     * predicates' columns when the source is a plain table and the server answers the statistics action.
     *
     * @return fraction of rows kept, in [0, 1]
     */
    private double filterSelectivity() {
        if (this.pdPredicates.length == 0) {
            return 1.0;
        }
        ServerColumnStatistics.Answer stats = ServerColumnStatistics.describable(this.table.getName())
                ? ServerColumnStatistics.fetch(this.configuration, this.table.getName(),
                        ScanSelectivity.columns(this.pdPredicates))
                : ServerColumnStatistics.Answer.NONE;
        return ScanSelectivity.of(this.pdPredicates, stats);
    }

    /**
     * Returns whether a pushed aggregation has aggregate functions and no grouping keys.
     *
     * @param aggregation pushed aggregation, or null
     * @return true when the scan yields exactly one row
     */
    static boolean isGlobalAggregation(PushAggregation aggregation) {
        return aggregation != null
                && (aggregation.getGroupByColumns() == null || aggregation.getGroupByColumns().length == 0)
                && aggregation.getColumnExpressions() != null
                && aggregation.getColumnExpressions().length > 0;
    }
}
