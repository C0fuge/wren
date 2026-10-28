package pro.surpin.data.arrowflight.client.spark.read;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pro.surpin.data.arrowflight.client.Client;
import pro.surpin.data.arrowflight.client.Configuration;
import pro.surpin.data.arrowflight.client.spark.security.FlightSessionTokens;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Column statistics from the Flight server's {@code arrowflight.table_statistics} action: row count and, per
 * column, the bounds and null count from the Parquet footers (and distinct counts once the server estimates them).
 * Answered without planning a query. A server without the action, a table it cannot describe, or a source that is
 * not a plain {@code schema.table} yields {@link Answer#NONE}: statistics only refine estimates, never results.
 */
final class ServerColumnStatistics {
    /** Name of the server's Flight action. */
    static final String ACTION = "arrowflight.table_statistics";
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerColumnStatistics.class);
    private static final ObjectMapper JSON = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private static final Pattern PLAIN_TABLE = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*\\.[A-Za-z_][A-Za-z0-9_]*");

    /**
     * Statistics of one column, as the server sends them.
     *
     * @param min smallest value as a number (decimals by value, dates as epoch days, timestamps as epoch seconds),
     *            or null when unknown (strings, missing footer statistics)
     * @param max largest value, or null when unknown
     * @param nulls number of nulls, or -1 when unknown
     * @param ndv estimated number of distinct values, or null when not estimated
     */
    record Column(Double min, Double max, long nulls, Long ndv) {
        /**
         * Tells whether both bounds are known and form a range.
         *
         * @return true with usable bounds
         */
        boolean hasBounds() {
            return min != null && max != null && !min.isNaN() && !max.isNaN() && min <= max;
        }
    }

    /**
     * The part of the server's answer used here.
     *
     * @param rows row count, -1 when unknown
     * @param columns statistics by column name, possibly null
     */
    record Answer(long rows, Map<String, Column> columns) {
        /** No statistics. */
        static final Answer NONE = new Answer(-1, Map.of());

        /**
         * Looks a column up, ignoring case as the server does.
         *
         * @param name column name
         * @return its statistics, or null
         */
        Column column(String name) {
            if (columns == null || name == null) {
                return null;
            }
            Column exact = columns.get(name);
            if (exact != null) {
                return exact;
            }
            for (Map.Entry<String, Column> entry : columns.entrySet()) {
                if (entry.getKey().toLowerCase(Locale.ROOT).equals(name.toLowerCase(Locale.ROOT))) {
                    return entry.getValue();
                }
            }
            return null;
        }
    }

    private ServerColumnStatistics() {
    }

    /**
     * Tells whether a scan source can be described by the action.
     *
     * @param source the scan's source, e.g. {@code schema.table}, or null
     * @return true for a plain unquoted {@code schema.table}
     */
    static boolean describable(String source) {
        return source != null && PLAIN_TABLE.matcher(source).matches();
    }

    /**
     * Fetches the statistics of the given columns.
     *
     * @param configuration connection configuration (a shipped Kerberos session is used when present)
     * @param source table as {@code schema.table}
     * @param columns names of the columns of interest
     * @return the server's answer, or {@link Answer#NONE}
     */
    static Answer fetch(Configuration configuration, String source, Collection<String> columns) {
        if (!describable(source) || columns.isEmpty()) {
            return Answer.NONE;
        }
        int dot = source.indexOf('.');
        try {
            Map<String, Object> request = new LinkedHashMap<>();
            request.put("schema", source.substring(0, dot));
            request.put("table", source.substring(dot + 1));
            request.put("columns", List.copyOf(columns));
            Client client = FlightSessionTokens.connect(configuration);
            Iterator<Result> results = client.getFlightClient().doAction(
                    new Action(ACTION, JSON.writeValueAsBytes(request)), client.callOptions());
            if (!results.hasNext()) {
                return Answer.NONE;
            }
            Answer answer = parse(new String(results.next().getBody(), StandardCharsets.UTF_8));
            while (results.hasNext()) {
                results.next();
            }
            return answer;
        } catch (FlightRuntimeException e) {
            // UNIMPLEMENTED: no such action, or this node does not see every file; NOT_FOUND: no such table.
            if (e.status().code() != CallStatus.UNIMPLEMENTED.code()
                    && e.status().code() != CallStatus.NOT_FOUND.code()) {
                LOGGER.warn("Table statistics of {} unavailable: {}", source, e.getMessage());
            }
            return Answer.NONE;
        } catch (JsonProcessingException | RuntimeException e) {
            LOGGER.warn("Table statistics of {} unavailable: {}", source, e.toString());
            return Answer.NONE;
        }
    }

    /**
     * Parses the server's JSON answer.
     *
     * @param json answer body
     * @return rows and columns
     * @throws JsonProcessingException when the body is not the expected JSON
     */
    static Answer parse(String json) throws JsonProcessingException {
        Answer answer = JSON.readValue(json, Answer.class);
        return answer == null ? Answer.NONE : answer;
    }
}
