package pro.surpin.data.arrowflight.client.spark.security;

import pro.surpin.data.arrowflight.client.Client;
import pro.surpin.data.arrowflight.client.Configuration;
import pro.surpin.data.arrowflight.server.KerberosTestAuthenticator;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kerby.kerberos.kerb.server.SimpleKdcServer;
import org.apache.spark.SparkConf;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;

import scala.Option;

import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.PrivilegedExceptionAction;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Real Kerberos end to end, without Spark's token manager: an in-process KDC (Apache Kerby), a TLS
 * Flight server with the server's GSS acceptor on a {@code flight/localhost} keytab login, the
 * provider obtaining a session with SPNEGO under a client keytab login (the driver), and an
 * executor without any Kerberos login connecting with that session.
 *
 * <p>Order matters: the client core writes its SPNEGO header on a gRPC executor thread, which
 * sees the process login user or the Kerberos login of the {@code doAs} it was created in, not the
 * caller's (see Open questions in {@code docs/STATUS.md}). So the case without any login runs
 * before gRPC threads are created inside the driver's {@code doAs}, and the client core's own
 * SPNEGO is exercised with a process login.
 */
@Tag("integration")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class FlightKerberosKdcTest {
    private static final String REALM = "EXAMPLE.COM";
    private static final String CLIENT = "alice@" + REALM;
    private static final String SERVICE = "flight/localhost@" + REALM;
    private static final byte[] SECRET = new byte[32];

    @TempDir
    static Path dir;
    private static SimpleKdcServer kdc;
    private static String previousKrb5Conf;
    private static UserGroupInformation driver;
    private static KerberosFlightServerFixture server;

    @BeforeAll
    static void start() throws Exception {
        int port;
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }
        kdc = new SimpleKdcServer();
        kdc.setWorkDir(Files.createDirectories(dir.resolve("kdc")).toFile());
        kdc.setKdcHost("localhost");
        kdc.setKdcRealm(REALM);
        kdc.setAllowUdp(false);
        kdc.setKdcTcpPort(port);
        kdc.init();
        kdc.start();
        Path clientKeytab = dir.resolve("alice.keytab");
        Path serviceKeytab = dir.resolve("flight.keytab");
        kdc.createAndExportPrincipals(clientKeytab.toFile(), "alice");
        kdc.createAndExportPrincipals(serviceKeytab.toFile(), "flight/localhost");
        kdc.createPrincipal("other/localhost", "other-password");
        Path krb5 = dir.resolve("krb5.conf");
        Files.writeString(krb5, "[libdefaults]\n default_realm = " + REALM + "\n"
                + " udp_preference_limit = 1\n dns_lookup_kdc = false\n dns_lookup_realm = false\n"
                + " dns_canonicalize_hostname = false\n rdns = false\n"
                + "[realms]\n " + REALM + " = {\n  kdc = localhost:" + port + "\n }\n");
        previousKrb5Conf = System.setProperty("java.security.krb5.conf", krb5.toString());

        org.apache.hadoop.conf.Configuration hadoop = new org.apache.hadoop.conf.Configuration(false);
        hadoop.set("hadoop.security.authentication", "kerberos");
        hadoop.set("hadoop.security.auth_to_local", "RULE:[1:$1] RULE:[2:$1] DEFAULT");
        UserGroupInformation.setConfiguration(hadoop);
        driver = UserGroupInformation.loginUserFromKeytabAndReturnUGI(CLIENT, clientKeytab.toString());
        UserGroupInformation service = UserGroupInformation.loginUserFromKeytabAndReturnUGI(SERVICE,
                serviceKeytab.toString());
        server = new KerberosFlightServerFixture(KerberosTestAuthenticator.gss(service, SECRET));
    }

    @AfterAll
    static void stop() throws Exception {
        if (server != null) {
            server.close();
        }
        if (kdc != null) {
            kdc.stop();
        }
        UserGroupInformation.reset();
        UserGroupInformation.setConfiguration(new org.apache.hadoop.conf.Configuration(false));
        if (previousKrb5Conf == null) {
            System.clearProperty("java.security.krb5.conf");
        } else {
            System.setProperty("java.security.krb5.conf", previousKrb5Conf);
        }
    }

    /**
     * Spark settings of the driver for the fixture server.
     *
     * @param servicePrincipal Flight service principal to request a ticket for
     * @return settings
     * @throws Exception on truststore failure
     */
    private static SparkConf conf(String servicePrincipal) throws Exception {
        Path truststore = dir.resolve("truststore.p12");
        server.writeTruststore(truststore, "changeit");
        return new SparkConf(false)
                .set(FlightDelegationTokenProvider.SERVERS, "localhost:" + server.port())
                .set(FlightDelegationTokenProvider.SERVICE_PRINCIPAL, servicePrincipal)
                .set(FlightDelegationTokenProvider.TLS_TRUSTSTORE_JKS, truststore.toString())
                .set(FlightDelegationTokenProvider.TLS_TRUSTSTORE_PASS, "changeit");
    }

    /**
     * Connector configuration of a Kerberos read or write.
     *
     * @param truststore truststore with the server certificate
     * @return configuration
     * @throws Exception on truststore failure
     */
    private static Configuration readConfig(Path truststore) throws Exception {
        server.writeTruststore(truststore, "changeit");
        Configuration config = new Configuration("localhost", server.port(), truststore.toString(), "changeit",
                "", "", "");
        config.setKerberosServicePrincipal(SERVICE);
        return config;
    }

    /**
     * Reads the principal from a session payload ({@code 1\n<expiry ms>\n<principal>}).
     *
     * @param session signed bearer session
     * @return principal the server authenticated
     */
    private static String principal(String session) {
        String payload = new String(Base64.getUrlDecoder().decode(session.substring(0, session.indexOf('.'))),
                StandardCharsets.UTF_8);
        return payload.split("\n", 3)[2];
    }

    @Test
    @Order(2)
    void driverObtainsASessionWithARealTicketAndAnExecutorWithoutLoginConnectsWithIt() throws Exception {
        Credentials creds = new Credentials();
        FlightDelegationTokenProvider provider = new FlightDelegationTokenProvider();
        Option<Object> next = driver.doAs((PrivilegedExceptionAction<Option<Object>>) () ->
                provider.obtainDelegationTokens(new org.apache.hadoop.conf.Configuration(false), conf(SERVICE),
                        creds));
        assertTrue(next.isDefined());
        assertEquals(1, creds.numberOfTokens());
        String session = new String(creds.getToken(FlightSessionTokens.alias("localhost", server.port()))
                .getPassword(), StandardCharsets.UTF_8);
        assertEquals(CLIENT, principal(session), "the server authenticated the driver's Kerberos principal");
        assertEquals((Long) next.get(), FlightSessionTokens.expiresAt(session));

        Configuration config = readConfig(dir.resolve("job-truststore.p12"));
        UserGroupInformation executor = UserGroupInformation.createRemoteUser("executor");
        assertFalse(executor.hasKerberosCredentials());
        executor.addCredentials(creds);
        Configuration resolved = executor.doAs(
                (PrivilegedExceptionAction<Configuration>) () -> FlightSessionTokens.resolve(config));
        assertNotSame(config, resolved);
        assertEquals(session, resolved.getBearerToken());
        Client client = executor.doAs((PrivilegedExceptionAction<Client>) () -> FlightSessionTokens.connect(config));
        assertNotNull(client);
        client.close();
    }

    @Test
    @Order(3)
    void ticketForAnotherServiceIsRejectedAndNothingIsShipped() throws Exception {
        Credentials creds = new Credentials();
        SparkConf conf = conf("other/localhost@" + REALM);
        assertThrows(RuntimeException.class, () -> driver.doAs((PrivilegedExceptionAction<Option<Object>>) () ->
                new FlightDelegationTokenProvider().obtainDelegationTokens(
                        new org.apache.hadoop.conf.Configuration(false), conf, creds)));
        assertEquals(0, creds.numberOfTokens());
    }

    @Test
    @Order(1)
    void executorWithoutSessionAndWithoutLoginFailsWithWhatIsMissing() throws Exception {
        Configuration config = readConfig(dir.resolve("bare-truststore.p12"));
        UserGroupInformation executor = UserGroupInformation.createRemoteUser("executor-without-session");
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> executor.doAs(
                (PrivilegedExceptionAction<Client>) () -> FlightSessionTokens.connect(config)));
        assertTrue(e.getMessage().contains("No Flight session for this server"), e.getMessage());
    }

    /**
     * Without a shipped session, the client core's own SPNEGO uses the process login (as Spark sets
     * it for {@code --principal/--keytab}); a {@code doAs} login alone is not enough, see the class
     * comment. Runs last: it replaces the process login user.
     */
    @Test
    @Order(4)
    void driverWithAKeytabProcessLoginConnectsWithItsOwnTicketWhenNoSessionIsShipped() throws Exception {
        Configuration config = readConfig(dir.resolve("driver-truststore.p12"));
        UserGroupInformation.setLoginUser(driver);
        assertSame(config, FlightSessionTokens.resolve(config), "no session in the driver's credentials");
        Client client = FlightSessionTokens.connect(config);
        assertNotNull(client);
        client.close();
    }
}
