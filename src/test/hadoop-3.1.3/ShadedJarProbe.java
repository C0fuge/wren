import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Reads a table through the "flight" data source with nothing but Spark, a Hadoop 3.1.3 client
 * and the shaded connector jar on the class path (run by ShadedJarHadoop313IT with the JDK's
 * source launcher, so it uses Spark's API only and is not compiled by the main build).
 * Prints one {@code PROBE ...} line per check; any exception fails the run.
 *
 * <p>Arguments: host, port, table.
 */
public final class ShadedJarProbe {

    private ShadedJarProbe() {
    }

    /**
     * Runs the reads.
     *
     * @param args host, port, table
     */
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("ShadedJarProbe")
                .master("local[2]")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .config("spark.driver.host", "127.0.0.1")
                .config("spark.ui.enabled", "false")
                .getOrCreate();
        try {
            System.out.println("PROBE hadoop=" + org.apache.hadoop.util.VersionInfo.getVersion()
                    + " spark=" + spark.version());
            Dataset<Row> df = spark.read().format("flight")
                    .option("host", args[0]).option("port", args[1])
                    .option("user", "test").option("password", "test")
                    .option("table", args[2])
                    .load();
            // Columnar read of every row (no pushdown), then a pushed filter and aggregate.
            Row all = df.selectExpr("count(*)", "sum(id)").first();
            System.out.println("PROBE all=" + all.getLong(0) + "," + all.getLong(1));
            long filtered = df.where("id > 100").count();
            System.out.println("PROBE filtered=" + filtered);
            List<Row> groups = df.groupBy("tinyint_col").count().collectAsList();
            System.out.println("PROBE groups=" + groups.size());
            System.out.println("PROBE source=" + connectorOrigin());
            System.out.println("PROBE epoll=" + relocatedEpoll());
        } finally {
            spark.stop();
        }
    }

    /** Whether the relocated Netty loads its (renamed) native epoll library, or why not. */
    private static String relocatedEpoll() {
        try {
            Class<?> epoll = Class.forName("pro.surpin.data.arrowflight.shaded.io.netty.channel.epoll.Epoll");
            if ((Boolean) epoll.getMethod("isAvailable").invoke(null)) {
                return "true";
            }
            return "false " + epoll.getMethod("unavailabilityCause").invoke(null);
        } catch (ReflectiveOperationException e) {
            return "false " + e;
        }
    }

    private static String connectorOrigin() {
        try {
            Class<?> c = Class.forName("pro.surpin.data.arrowflight.client.spark.FlightSource");
            return String.valueOf(c.getProtectionDomain().getCodeSource().getLocation());
        } catch (ClassNotFoundException e) {
            return "missing";
        }
    }
}
