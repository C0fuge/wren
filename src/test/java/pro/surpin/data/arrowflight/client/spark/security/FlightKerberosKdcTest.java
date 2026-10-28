package pro.surpin.data.arrowflight.client.spark.security;

import pro.surpin.data.arrowflight.client.Client;
import pro.surpin.data.arrowflight.client.Configuration;
import pro.surpin.data.arrowflight.server.KerberosTestAuthenticator;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
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

import java.nio.charset.StandardCharsets;
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
 * Real Kerberos end to end, without Spark's token manager: an in-process KDC ({@link KerberosKdcFixture}), a TLS
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
    private static final String CLIENT = KerberosKdcFixture.CLIENT;
    private static final String SERVICE = KerberosKdcFixture.SERVICE;
    private static final byte[] SECRET = new byte[32];

    @TempDir
    static Path dir;
    private static KerberosKdcFixture kdc;
    private static UserGroupInformation driver;
    private static KerberosFlightServerFixture server;

    @BeforeAll
    static void start() throws Exception {
        kdc = new KerberosKdcFixture(dir);
        driver = kdc.loginClient();
        server = new KerberosFlightServerFixture(KerberosTestAuthenticator.gss(kdc.loginService(), SECRET));
    }

    @AfterAll
    static void stop() throws Exception {
        if (server != null) {
            server.close();
        }
        if (kdc != null) {
            kdc.close();
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
        SparkConf conf = conf(KerberosKdcFixture.OTHER_SERVICE);
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
