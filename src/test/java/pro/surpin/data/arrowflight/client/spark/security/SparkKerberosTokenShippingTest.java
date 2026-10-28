package pro.surpin.data.arrowflight.client.spark.security;

import pro.surpin.data.arrowflight.server.KerberosTestAuthenticator;
import pro.surpin.data.arrowflight.server.TestFlightServerHelper;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.apache.spark.deploy.SparkHadoopUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Spark's own delegation token manager ships the Flight session: a {@code local-cluster} Spark
 * (separate executor JVMs, which only Kerberos-less sessions can authenticate) with
 * {@code spark.kerberos.principal/keytab} against the in-process KDC, and a TLS Flight server with
 * the server's GSS acceptor in front of the in-process Flight SQL producer. The driver's token
 * manager runs {@link FlightDelegationTokenProvider} under its keytab login and sends the session
 * to the executors, which read the table with it: no SPNEGO handshake happens during the read.
 *
 * <p>The producer answers {@code complete} (the TLS server has no result-mode middleware), so
 * endpoints carry no location and executors read over the TLS connection they opened.
 */
@Tag("integration")
@Tag("spark")
class SparkKerberosTokenShippingTest {
    private static final byte[] SECRET = new byte[32];
    private static final String TABLE = "test_schema.test_table";
    private static final AtomicInteger NEGOTIATE = new AtomicInteger();
    private static final AtomicInteger BEARER = new AtomicInteger();

    @TempDir
    static Path dir;
    private static KerberosKdcFixture kdc;
    private static TestFlightServerHelper helper;
    private static KerberosFlightServerFixture server;
    private static Path truststore;
    private static String previousTesting;
    private static String previousTestHome;
    private static SparkSession spark;

    @BeforeAll
    static void start() throws Exception {
        // Its one-time initialisation resets Hadoop's security settings from the system properties.
        SparkHadoopUtil.get();
        kdc = new KerberosKdcFixture(dir);
        helper = TestFlightServerHelper.builder()
                .dataDir(Paths.get("src/test/resources/test_db").toAbsolutePath().normalize().toString())
                .resultMode("complete")
                .start();
        server = new KerberosFlightServerFixture(counting(KerberosTestAuthenticator.gss(kdc.loginService(), SECRET)),
                helper.flightSqlProducer);
        truststore = dir.resolve("truststore.p12");
        server.writeTruststore(truststore, "changeit");

        // With spark.testing, standalone executors are launched with this JVM's class path.
        previousTesting = System.setProperty("spark.testing", "true");
        // Workers need a Spark home; the executor class path comes from this JVM, not from its jars.
        previousTestHome = System.setProperty("spark.test.home",
                Files.createDirectories(dir.resolve("spark-home")).toString());
        String executorOptions = "-Djava.security.krb5.conf=" + kdc.krb5Conf()
                + " --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
                + " --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED"
                + " --add-opens=java.base/sun.util.calendar=ALL-UNNAMED";
        spark = SparkSession.builder()
                .appName("SparkKerberosTokenShippingTest")
                .master(TokenManagingLocalClusterManager.MASTER)
                .config("spark.driver.bindAddress", "127.0.0.1")
                .config("spark.driver.host", "127.0.0.1")
                .config("spark.ui.enabled", "false")
                .config("spark.executorEnv.SPARK_TESTING", "1")
                .config("spark.executorEnv.SPARK_SCALA_VERSION", "2.12")
                .config("spark.executor.extraJavaOptions", executorOptions)
                .config("spark.hadoop.hadoop.security.authentication", "kerberos")
                .config("spark.hadoop.hadoop.security.auth_to_local", "RULE:[1:$1] RULE:[2:$1] DEFAULT")
                .config("spark.kerberos.principal", KerberosKdcFixture.CLIENT)
                .config("spark.kerberos.keytab", kdc.clientKeytab().toString())
                .config("spark.security.credentials.hadoopfs.enabled", "false")
                .config("spark.security.credentials.hbase.enabled", "false")
                .config("spark.security.credentials.hive.enabled", "false")
                .config(FlightDelegationTokenProvider.SERVERS, "localhost:" + server.port())
                .config(FlightDelegationTokenProvider.SERVICE_PRINCIPAL, KerberosKdcFixture.SERVICE)
                .config(FlightDelegationTokenProvider.TLS_TRUSTSTORE_JKS, truststore.toString())
                .config(FlightDelegationTokenProvider.TLS_TRUSTSTORE_PASS, "changeit")
                .getOrCreate();
    }

    @AfterAll
    static void stop() throws Exception {
        try {
            if (spark != null) {
                spark.stop();
            }
            if (server != null) {
                server.close();
            }
            if (helper != null) {
                helper.close();
            }
        } finally {
            restore("spark.testing", previousTesting);
            restore("spark.test.home", previousTestHome);
            if (kdc != null) {
                kdc.close();
            }
        }
    }

    /**
     * Restores a system property.
     *
     * @param key property name
     * @param previous value before the test, or null when it was not set
     */
    private static void restore(String key, String previous) {
        if (previous == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, previous);
        }
    }

    /**
     * Counts the calls the server authenticates with a SPNEGO ticket and with a bearer session.
     *
     * @param delegate the server's authenticator
     * @return counting authenticator
     */
    private static CallHeaderAuthenticator counting(CallHeaderAuthenticator delegate) {
        return headers -> {
            countAuthorization(headers);
            return delegate.authenticate(headers);
        };
    }

    /**
     * Counts one call by its {@code Authorization} scheme.
     *
     * @param headers incoming call headers
     */
    private static void countAuthorization(CallHeaders headers) {
        String authorization = headers.get("authorization");
        if (authorization == null) {
            return;
        }
        if (authorization.startsWith("Negotiate ")) {
            NEGOTIATE.incrementAndGet();
        } else if (authorization.startsWith("Bearer ")) {
            BEARER.incrementAndGet();
        }
    }

    @Test
    void executorsReadWithTheSessionTheTokenManagerShipped() {
        assertTrue(NEGOTIATE.get() > 0, "the driver's token manager obtained a session with SPNEGO");
        int handshakes = NEGOTIATE.get();
        int bearers = BEARER.get();

        Dataset<Row> df = spark.read().format("flight")
                .option("host", "localhost")
                .option("port", String.valueOf(server.port()))
                .option("tls.enabled", "true")
                .option("tls.truststore.jksFile", truststore.toString())
                .option("tls.truststore.pass", "changeit")
                .option("kerberos.servicePrincipal", KerberosKdcFixture.SERVICE)
                .option("table", TABLE)
                .load();
        assertEquals(1000L, df.count(), "the sample table has 1000 rows");
        assertEquals(10L, df.filter("id < 10").count());

        assertEquals(handshakes, NEGOTIATE.get(), "no SPNEGO handshake during the read");
        assertTrue(BEARER.get() > bearers, "the read authenticated with the shipped session");
    }
}
