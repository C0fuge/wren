package pro.surpin.data.arrowflight.client.spark;

import pro.surpin.data.arrowflight.client.expr.Expr;
import pro.surpin.data.arrowflight.client.expr.SqlRenderer;
import pro.surpin.data.arrowflight.client.spark.read.FlightScan;
import org.apache.spark.sql.catalyst.expressions.And;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.AttributeSet;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Like;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.Not;
import org.apache.spark.sql.catalyst.plans.logical.Filter;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.catalyst.rules.Rule;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.runtime.AbstractPartialFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * Optimizer rule that sends {@code LIKE} filters Spark cannot push to the Flight server as well.
 *
 * <p>Spark translates only {@code LIKE} patterns of the shapes {@code 'abc%'}, {@code '%abc'} and {@code '%abc%'}
 * into connector predicates; a filter such as TPC-H Q13's {@code o_comment NOT LIKE '%special%requests%'} stays in
 * Spark, so every row of the column crosses the wire. After Spark's scan push-down this rule moves each such
 * {@code [NOT] LIKE} conjunct of a Filter directly above a Flight scan without a pushed aggregation into the scan's
 * WHERE clause ({@link FlightScan#pushConditions(List, List)}, which plans the scan again) and removes it from the
 * Filter; when a Project above the Filter shows that a scan column was needed only by moved conjuncts (Q13's
 * {@code o_comment}), the scan stops reading it. The server evaluates these patterns exactly as Spark does, as for
 * the {@code LIKE} shapes Spark pushes itself.
 *
 * <p>Only patterns with the same meaning in Spark and in the server's SQL are sent: a non-null string literal of
 * literal characters and {@code %} wildcards (Spark's escapes of {@code %}, {@code _} and the escape character are
 * resolved), on a plain string column of the scan. Patterns with {@code _} (one character in Spark; the server's
 * notion of a character is not checked here) or invalid escapes stay in Spark only. Spark SQL setting
 * {@value #ENABLED} ({@code true} by default) turns the rule off. Registered with {@link FlightOptimizations}.
 */
public final class FlightLikePushdown extends Rule<LogicalPlan> {
    /** Spark SQL setting: {@code false} keeps {@code LIKE} filters Spark cannot push in Spark only. */
    public static final String ENABLED = "spark.flight.likePushdown";

    private static final Logger LOGGER = LoggerFactory.getLogger(FlightLikePushdown.class);
    private static final FlightLikePushdown INSTANCE = new FlightLikePushdown();

    private FlightLikePushdown() {
    }

    /**
     * Returns the rule.
     *
     * @return the shared instance
     */
    public static FlightLikePushdown instance() {
        return INSTANCE;
    }

    /**
     * Moves the pushable {@code LIKE} conjuncts of Filters over Flight scans into those scans and drops the columns
     * only they used from the scans.
     *
     * @param plan the logical plan
     * @return the plan without the pushed conjuncts
     */
    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        if (!"true".equalsIgnoreCase(SQLConf.get().getConfString(ENABLED, "true").trim())) {
            return plan;
        }
        // Top down: a Project above the Filter is seen first and tells which scan columns stay needed.
        return plan.transformDown(new AbstractPartialFunction<LogicalPlan, LogicalPlan>() {
            @Override
            public boolean isDefinedAt(LogicalPlan node) {
                return filterOverScan(node instanceof Project project ? project.child() : node) != null;
            }

            @Override
            @SuppressWarnings("unchecked")
            public <A1 extends LogicalPlan, B1> B1 applyOrElse(A1 node, scala.Function1<A1, B1> otherwise) {
                if (!isDefinedAt(node)) {
                    return otherwise.apply(node);
                }
                if (node instanceof Project project) {
                    LogicalPlan child = push((Filter) project.child(), project.references());
                    return (B1) (child == project.child() ? project : new Project(project.projectList(), child));
                }
                return (B1) push((Filter) node, null);
            }
        });
    }

    private static DataSourceV2ScanRelation filterOverScan(LogicalPlan node) {
        if (node instanceof Filter filter && filter.child() instanceof DataSourceV2ScanRelation relation
                && relation.scan() instanceof FlightScan scan && !scan.aggregated()) {
            return relation;
        }
        return null;
    }

    /**
     * Pushes a Filter's {@code LIKE} conjuncts into the scan below it.
     *
     * @param filter a Filter directly above a Flight scan
     * @param above columns the parent Project uses, or null when every scan column stays (no Project)
     * @return the Filter with the remaining conjuncts over the (possibly narrower) scan, or the unchanged Filter
     */
    private static LogicalPlan push(Filter filter, AttributeSet above) {
        DataSourceV2ScanRelation relation = (DataSourceV2ScanRelation) filter.child();
        FlightScan scan = (FlightScan) relation.scan();
        List<Expression> conjuncts = new ArrayList<>();
        splitConjuncts(filter.condition(), conjuncts);
        SqlRenderer renderer = scan.renderer();
        List<String> conditions = new ArrayList<>();
        List<Expression> remaining = new ArrayList<>();
        for (Expression conjunct : conjuncts) {
            String rendered = render(conjunct, relation, renderer);
            if (rendered != null) {
                conditions.add(rendered);
            } else {
                remaining.add(conjunct);
            }
        }
        if (conditions.isEmpty()) {
            return filter;
        }
        List<AttributeReference> output = JavaConverters.seqAsJavaListConverter(relation.output()).asJava();
        List<AttributeReference> kept = output;
        if (above != null) {
            AttributeSet needed = above;
            for (Expression conjunct : remaining) {
                needed = needed.$plus$plus(conjunct.references());
            }
            AttributeSet used = needed;
            kept = output.stream().filter(used::contains).toList();
            if (kept.isEmpty()) {
                kept = output.subList(0, 1); // a scan reads at least one column
            }
        }
        List<String> keptNames = kept.size() == output.size() ? null
                : kept.stream().map(AttributeReference::name).toList();
        // The narrower node is built before the scan changes, so a failure leaves plan and scan as they were.
        LogicalPlan child = keptNames == null ? relation : withOutput(relation, kept);
        if (!scan.pushConditions(conditions, keptNames)) {
            return filter;
        }
        LOGGER.debug("Pushed {} LIKE conditions to the Flight scan of {}", conditions.size(), scan.tableName());
        if (remaining.isEmpty()) {
            return child;
        }
        Expression condition = remaining.get(0);
        for (Expression conjunct : remaining.subList(1, remaining.size())) {
            condition = new And(condition, conjunct);
        }
        return new Filter(condition, child);
    }

    /**
     * Copies a scan relation with other output columns. The constructor's other parameters differ between Spark
     * versions (3.3 has no ordering), so the copy goes through the node's own fields.
     *
     * @param relation the scan relation
     * @param output the new output, a subset of the relation's
     * @return the copy
     */
    private static LogicalPlan withOutput(DataSourceV2ScanRelation relation, List<AttributeReference> output) {
        Object[] fields = new Object[relation.productArity()];
        for (int i = 0; i < fields.length; i++) {
            fields[i] = relation.productElement(i);
        }
        fields[2] = JavaConverters.asScalaBufferConverter(output).asScala().toList(); // (relation, scan, output, ...)
        return relation.makeCopy(fields);
    }

    private static void splitConjuncts(Expression expression, List<Expression> out) {
        if (expression instanceof And and) {
            splitConjuncts(and.left(), out);
            splitConjuncts(and.right(), out);
        } else {
            out.add(expression);
        }
    }

    /**
     * Renders a {@code [NOT] LIKE} conjunct over a scan column as server SQL.
     *
     * @param conjunct one conjunct of the Filter's condition
     * @param relation the scan relation below the Filter
     * @param renderer the scan's SQL renderer
     * @return the WHERE condition, or null when the conjunct is no pushable {@code LIKE}
     */
    static String render(Expression conjunct, DataSourceV2ScanRelation relation, SqlRenderer renderer) {
        boolean negated = conjunct instanceof Not;
        Expression inner = negated ? ((Not) conjunct).child() : conjunct;
        if (!(inner instanceof Like like) || !(like.right() instanceof Literal literal) || literal.value() == null
                || !DataTypes.StringType.equals(literal.dataType())) {
            return null;
        }
        String column = scanColumn(like.left(), relation);
        String pattern = column == null ? null : sqlPattern(literal.value().toString(), like.escapeChar());
        if (pattern == null) {
            return null;
        }
        String rendered = renderer.quoteIdentifier(column) + " like " + renderer.render(new Expr.Literal(pattern))
                + " escape '\\'";
        return negated ? "not (" + rendered + ")" : rendered;
    }

    private static String scanColumn(Expression expression, DataSourceV2ScanRelation relation) {
        if (!(expression instanceof Attribute attribute) || !DataTypes.StringType.equals(attribute.dataType())) {
            return null;
        }
        for (AttributeReference column : JavaConverters.seqAsJavaListConverter(relation.output()).asJava()) {
            if (column.exprId().equals(attribute.exprId())) {
                return column.name();
            }
        }
        return null;
    }

    /**
     * Converts a Spark {@code LIKE} pattern into the same pattern for the server's {@code LIKE ... ESCAPE '\'}.
     *
     * @param pattern Spark pattern
     * @param escape Spark escape character
     * @return the server pattern, or null when the pattern has {@code _} wildcards or an invalid escape
     */
    static String sqlPattern(String pattern, char escape) {
        StringBuilder out = new StringBuilder(pattern.length() + 8);
        for (int i = 0; i < pattern.length(); i++) {
            char c = pattern.charAt(i);
            if (c == escape) {
                // As Spark: the escape character may only precede _, % or itself.
                if (i + 1 >= pattern.length()) {
                    return null;
                }
                char next = pattern.charAt(++i);
                if (next != '_' && next != '%' && next != escape) {
                    return null;
                }
                appendLiteral(out, next);
            } else if (c == '%') {
                out.append('%');
            } else if (c == '_') {
                return null;
            } else {
                appendLiteral(out, c);
            }
        }
        return out.toString();
    }

    private static void appendLiteral(StringBuilder out, char c) {
        if (c == '\\' || c == '%' || c == '_') {
            out.append('\\');
        }
        out.append(c);
    }
}
