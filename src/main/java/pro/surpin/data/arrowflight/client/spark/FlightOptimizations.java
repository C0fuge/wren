package pro.surpin.data.arrowflight.client.spark;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.rules.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * Adds the connector's optimizer rules ({@link FlightPushedCounts}, {@link FlightLikePushdown}) to Spark's
 * {@code experimental.extraOptimizations} of a session.
 *
 * <p>Those run at the end of the optimisation of each query and subquery, after Spark's scan push-down. A job that
 * sets {@code spark.experimental.extraOptimizations} itself after reading a Flight table replaces the rules; the
 * connector adds them again whenever it resolves a table.
 */
public final class FlightOptimizations {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlightOptimizations.class);

    private FlightOptimizations() {
    }

    /**
     * Adds the rules to the active (else the default) session unless they are there already. Any failure is logged
     * and leaves the session unchanged.
     */
    public static void register() {
        scala.Option<SparkSession> session = SparkSession.getActiveSession();
        if (session.isEmpty()) {
            session = SparkSession.getDefaultSession();
        }
        if (!session.isEmpty()) {
            register(session.get());
        }
    }

    /**
     * Adds the rules to the session's extra optimizations unless they are there already. Any failure is logged and
     * leaves the session unchanged.
     *
     * @param session the Spark session
     */
    public static void register(SparkSession session) {
        try {
            // Reflection: SparkSession.experimental is not part of Spark 4's API class (only of its classic session).
            Object experimental = session.getClass().getMethod("experimental").invoke(session);
            synchronized (experimental) {
                Method getter = experimental.getClass().getMethod("extraOptimizations");
                List<Object> rules = new ArrayList<>(JavaConverters.seqAsJavaListConverter(
                        (scala.collection.Seq<?>) getter.invoke(experimental)).asJava());
                int before = rules.size();
                add(rules, FlightPushedCounts.instance());
                add(rules, FlightLikePushdown.instance());
                if (rules.size() == before) {
                    return;
                }
                Method setter = null;
                for (Method method : experimental.getClass().getMethods()) {
                    if ("extraOptimizations_$eq".equals(method.getName())) {
                        setter = method;
                    }
                }
                if (setter == null) {
                    throw new NoSuchMethodException("extraOptimizations_$eq");
                }
                setter.invoke(experimental, JavaConverters.asScalaBufferConverter(rules).asScala().toList());
            }
        } catch (ReflectiveOperationException | RuntimeException e) {
            LOGGER.warn("Could not register the connector's optimizer rules in the Spark session; with ANSI mode a"
                    + " pushed avg in a correlated scalar subquery may fail on Spark 3.5, and multi-wildcard LIKE"
                    + " filters are evaluated in Spark only", e);
        }
    }

    private static void add(List<Object> rules, Rule<LogicalPlan> rule) {
        if (rules.stream().noneMatch(rule.getClass()::isInstance)) {
            rules.add(rule);
        }
    }
}
