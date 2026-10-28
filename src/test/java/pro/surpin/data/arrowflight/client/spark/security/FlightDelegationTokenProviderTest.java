package pro.surpin.data.arrowflight.client.spark.security;

import pro.surpin.data.arrowflight.client.Client;
import pro.surpin.data.arrowflight.client.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkConf;
import org.apache.spark.security.HadoopDelegationTokenProvider;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import scala.Option;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.ServiceLoader;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Driver side (provider) and executor side (session instead of SPNEGO) against a TLS Flight server
 * running the server's Kerberos authenticator with a stub ticket check.
 */
@Tag("integration")
class FlightDelegationTokenProviderTest {
    private static KerberosFlightServerFixture server;

    @BeforeAll
    static void start() throws Exception {
        server = new KerberosFlightServerFixture();
    }

    @AfterAll
    static void stop() throws Exception {
        server.close();
    }

    /**
     * Spark settings for the fixture server.
     *
     * @return settings
     * @throws Exception on PEM failure
     */
    private static SparkConf conf(Path dir) throws Exception {
        Path truststore = dir.resolve("driver-truststore.p12");
        server.writeTruststore(truststore, "changeit");
        return new SparkConf(false)
                .set(FlightDelegationTokenProvider.SERVERS, "localhost:" + server.port())
                .set(FlightDelegationTokenProvider.SERVICE_PRINCIPAL, KerberosFlightServerFixture.SERVICE_PRINCIPAL)
                .set(FlightDelegationTokenProvider.TLS_TRUSTSTORE_JKS, truststore.toString())
                .set(FlightDelegationTokenProvider.TLS_TRUSTSTORE_PASS, "changeit");
    }

    @Test
    void isRegisteredWithSparksServiceLoader() {
        boolean found = false;
        for (HadoopDelegationTokenProvider provider : ServiceLoader.load(HadoopDelegationTokenProvider.class)) {
            found |= provider instanceof FlightDelegationTokenProvider && "flight".equals(provider.serviceName());
        }
        assertTrue(found);
    }

    @Test
    void isRequiredOnlyWithServersAndPrincipal() {
        FlightDelegationTokenProvider provider = new FlightDelegationTokenProvider();
        org.apache.hadoop.conf.Configuration hadoop = new org.apache.hadoop.conf.Configuration(false);
        assertFalse(provider.delegationTokensRequired(new SparkConf(false), hadoop));
        assertFalse(provider.delegationTokensRequired(new SparkConf(false)
                .set(FlightDelegationTokenProvider.SERVERS, "h:1"), hadoop));
        assertTrue(provider.delegationTokensRequired(new SparkConf(false)
                .set(FlightDelegationTokenProvider.SERVERS, "h:1")
                .set(FlightDelegationTokenProvider.SERVICE_PRINCIPAL, "flight/h@R"), hadoop));
    }

    @Test
    void parsesTheServerList() {
        List<String[]> servers = FlightDelegationTokenProvider.servers(" a.test:1, b.test:32010 ,,");
        assertEquals(2, servers.size());
        assertArrayEquals(new String[] {"a.test", "1"}, servers.get(0));
        assertArrayEquals(new String[] {"b.test", "32010"}, servers.get(1));
        assertThrows(IllegalArgumentException.class, () -> FlightDelegationTokenProvider.servers("a.test"));
        assertThrows(IllegalArgumentException.class, () -> FlightDelegationTokenProvider.servers("a.test:"));
        assertThrows(IllegalArgumentException.class, () -> FlightDelegationTokenProvider.servers("a.test:x"));
    }

    @Test
    void obtainsASessionAndExecutorsConnectWithItWithoutKerberos(@TempDir Path dir) throws Exception {
        Credentials creds = new Credentials();
        Option<Object> next = FlightDelegationTokenProvider.obtain(conf(dir), creds,
                principal -> {
                    assertEquals(KerberosFlightServerFixture.SERVICE_PRINCIPAL, principal);
                    return KerberosFlightServerFixture.negotiate();
                });
        assertTrue(next.isDefined());
        long expiresAt = (Long) next.get();
        assertTrue(expiresAt > System.currentTimeMillis() + 500_000, "sessions last ten minutes");
        String session = new String(creds.getToken(FlightSessionTokens.alias("localhost", server.port()))
                .getPassword(), StandardCharsets.UTF_8);
        assertEquals(expiresAt, FlightSessionTokens.expiresAt(session));

        // Executor: the job's truststore path does not exist here, only the certificate shipped with the
        // configuration; there is no Kerberos login, only the session in the credentials.
        Path truststore = dir.resolve("job-truststore.p12");
        server.writeTruststore(truststore, "changeit");
        Configuration config = new Configuration("localhost", server.port(), truststore.toString(), "changeit",
                "", "", "");
        config.setKerberosServicePrincipal(KerberosFlightServerFixture.SERVICE_PRINCIPAL);
        Files.delete(truststore);

        UserGroupInformation executor = UserGroupInformation.createRemoteUser("executor");
        executor.addCredentials(creds);
        Configuration resolved = executor.doAs(
                (PrivilegedExceptionAction<Configuration>) () -> FlightSessionTokens.resolve(config));
        assertNotSame(config, resolved);
        assertEquals(session, resolved.getBearerToken());
        assertArrayEquals(config.getCertificateBytes(), resolved.getCertificateBytes());
        Client client = executor.doAs((PrivilegedExceptionAction<Client>) () -> FlightSessionTokens.connect(config));
        assertNotNull(client);
        client.close();
    }

    @Test
    void failsClearlyWithoutSessionAndWithoutKerberosLogin() throws Exception {
        Configuration config = new Configuration("localhost", server.port(), true, false, "", "", "");
        config.setKerberosServicePrincipal(KerberosFlightServerFixture.SERVICE_PRINCIPAL);
        UserGroupInformation executor = UserGroupInformation.createRemoteUser("executor-without-session");
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> executor.doAs(
                (PrivilegedExceptionAction<Client>) () -> FlightSessionTokens.connect(config)));
        assertTrue(e.getMessage().contains("No Flight session for this server"), e.getMessage());
        assertTrue(e.getMessage().contains("spark.flight.kerberos.servers"), e.getMessage());
    }

    @Test
    void driverWithoutKerberosLoginFailsInsteadOfShippingNothing(@TempDir Path dir) throws Exception {
        UserGroupInformation driver = UserGroupInformation.createRemoteUser("driver-without-tgt");
        SparkConf conf = conf(dir);
        Credentials creds = new Credentials();
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> driver.doAs(
                (PrivilegedExceptionAction<Option<Object>>) () -> new FlightDelegationTokenProvider()
                        .obtainDelegationTokens(new org.apache.hadoop.conf.Configuration(false), conf, creds)));
        assertTrue(e.getMessage().contains("cannot authenticate"), e.getMessage());
        assertEquals(0, creds.numberOfTokens());
    }

    @Test
    void rejectedTicketYieldsNoSession(@TempDir Path dir) throws Exception {
        Credentials creds = new Credentials();
        assertThrows(RuntimeException.class, () -> FlightDelegationTokenProvider.obtain(conf(dir), creds,
                principal -> "Negotiate " + java.util.Base64.getEncoder().encodeToString(
                        "wrong".getBytes(StandardCharsets.UTF_8))));
        assertEquals(0, creds.numberOfTokens());
    }
}
