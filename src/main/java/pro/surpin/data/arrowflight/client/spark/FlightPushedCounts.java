package pro.surpin.data.arrowflight.client.spark;

import pro.surpin.data.arrowflight.client.spark.read.FlightScan;
import org.apache.spark.sql.catalyst.expressions.Alias;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.Coalesce;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression;
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum;
import org.apache.spark.sql.catalyst.plans.logical.Aggregate;
import org.apache.spark.sql.catalyst.plans.logical.Filter;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.catalyst.rules.Rule;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.JavaConverters;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractPartialFunction;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Optimizer rule that makes Spark's merge of pushed partial counts return 0, not null, on no rows.
 *
 * <p>When the connector accepts a partial aggregation, Spark merges the scan's partial {@code COUNT} columns with
 * {@code sum(...)}, which is null on no input rows where a count is 0. Spark 3.5 evaluates the aggregates of a
 * correlated scalar subquery on zero rows ({@code RewriteCorrelatedScalarSubquery}); for a pushed {@code avg} of a
 * decimal that is a division of null by null, which fails in ANSI mode ({@code [ARITHMETIC_OVERFLOW] Overflow in sum
 * of decimals}, TPC-H Q17). This rule wraps every such merge in {@code coalesce(sum(count), 0)}, which is the exact
 * count in every case. The connector pushes {@code SUM} only for decimal and floating types, so a {@code sum} of a
 * {@code bigint} column of a scan with a pushed aggregation is always a merged count.
 *
 * <p>The rule has to run after the subquery's own scan push-down and before the outer query rewrites the subquery:
 * only Spark's {@code experimental.extraOptimizations} run there (at the end of the optimisation of each subquery).
 * {@link FlightOptimizations#register()} adds the rule to the active session's list once; the connector calls it
 * whenever it resolves a table.
 */
public final class FlightPushedCounts extends Rule<LogicalPlan> {
    private static final FlightPushedCounts INSTANCE = new FlightPushedCounts();

    private FlightPushedCounts() {
    }

    /**
     * Returns the rule.
     *
     * @return the shared instance
     */
    public static FlightPushedCounts instance() {
        return INSTANCE;
    }

    /**
     * Wraps the merged pushed counts of every aggregate over a Flight scan with a pushed aggregation.
     *
     * @param plan the logical plan
     * @return the plan with {@code coalesce(sum(count), 0)} for each merged count
     */
    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        return plan.transformUp(new AbstractPartialFunction<LogicalPlan, LogicalPlan>() {
            @Override
            public boolean isDefinedAt(LogicalPlan node) {
                return node instanceof Aggregate aggregate && countColumns(aggregate.child()) != null;
            }

            @Override
            @SuppressWarnings("unchecked")
            public <A1 extends LogicalPlan, B1> B1 applyOrElse(A1 node, scala.Function1<A1, B1> otherwise) {
                if (!isDefinedAt(node)) {
                    return otherwise.apply(node);
                }
                Set<ExprId> counts = countColumns(((Aggregate) node).child());
                return (B1) node.mapExpressions(new AbstractFunction1<Expression, Expression>() {
                    @Override
                    public Expression apply(Expression expression) {
                        return wrap(expression, counts);
                    }
                });
            }
        });
    }

    /**
     * Finds the {@code bigint} output columns of a Flight scan with a pushed aggregation below Filters and Projects
     * that pass columns through.
     *
     * @param plan the aggregate's child
     * @return ids of the candidate count columns, or null when the plan is no such scan
     */
    private static Set<ExprId> countColumns(LogicalPlan plan) {
        if (plan instanceof Filter filter) {
            return countColumns(filter.child());
        }
        if (plan instanceof Project project) {
            Set<ExprId> below = countColumns(project.child());
            if (below == null) {
                return null;
            }
            Set<ExprId> passed = new HashSet<>();
            for (NamedExpression column : JavaConverters.seqAsJavaListConverter(project.projectList()).asJava()) {
                Expression source = column instanceof Alias alias ? alias.child() : (Expression) column;
                if (source instanceof Attribute attribute && below.contains(attribute.exprId())) {
                    passed.add(column.exprId());
                }
            }
            return passed;
        }
        if (plan instanceof DataSourceV2ScanRelation relation && relation.scan() instanceof FlightScan scan
                && scan.aggregated()) {
            Set<ExprId> ids = new HashSet<>();
            for (AttributeReference column : JavaConverters.seqAsJavaListConverter(relation.output()).asJava()) {
                if (DataTypes.LongType.equals(column.dataType())) {
                    ids.add(column.exprId());
                }
            }
            return ids;
        }
        return null;
    }

    /**
     * Wraps the merged counts in an expression, leaving those already wrapped.
     *
     * @param expression an aggregate's output expression or a part of it
     * @param counts ids of the scan's count columns
     * @return the expression with every merged count wrapped once
     */
    static Expression wrap(Expression expression, Set<ExprId> counts) {
        if (expression instanceof Coalesce coalesce && coalesce.children().size() == 2
                && isMergedCount(coalesce.children().head(), counts)) {
            return expression;
        }
        if (isMergedCount(expression, counts)) {
            List<Expression> children = List.of(expression, Literal.create(0L, DataTypes.LongType));
            return new Coalesce(JavaConverters.asScalaBufferConverter(children).asScala().toList());
        }
        return expression.mapChildren(new AbstractFunction1<Expression, Expression>() {
            @Override
            public Expression apply(Expression child) {
                return wrap(child, counts);
            }
        });
    }

    private static boolean isMergedCount(Expression expression, Set<ExprId> counts) {
        return expression instanceof AggregateExpression aggregate
                && !aggregate.isDistinct()
                && aggregate.filter().isEmpty()
                && aggregate.aggregateFunction() instanceof Sum sum
                && sum.child() instanceof Attribute column
                && counts.contains(column.exprId());
    }
}
