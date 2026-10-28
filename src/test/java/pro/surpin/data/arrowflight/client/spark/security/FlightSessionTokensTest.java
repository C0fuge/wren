package pro.surpin.data.arrowflight.client.spark.security;

import pro.surpin.data.arrowflight.client.Configuration;
import pro.surpin.data.arrowflight.server.KerberosTestAuthenticator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Executor side of Flight sessions in the job credentials. */
@Tag("unit")
class FlightSessionTokensTest {
    private static final byte[] SECRET = new byte[32];

    /**
     * Builds a session string with a given expiry (unsigned: only the client-side parsing is tested).
     *
     * @param expiresAt expiry in Unix milliseconds
     * @return session
     */
    private static String session(long expiresAt) {
        String payload = "1\n" + expiresAt + "\nalice@TEST.LOCAL";
        return Base64.getUrlEncoder().withoutPadding().encodeToString(payload.getBytes(StandardCharsets.UTF_8))
                + ".c2ln";
    }

    /**
     * A Kerberos configuration with request settings that must survive the copy.
     *
     * @return configuration
     */
    private static Configuration kerberos() {
        Configuration config = new Configuration("flight.test", 32010, true, true, "", "", "");
        config.setKerberosServicePrincipal("flight/flight.test@TEST.LOCAL");
        config.setDefaultSchema("tpch");
        config.setRoutingTag("tag");
        config.setRoutingQueue("queue");
        config.setMaxRetries(7);
        config.setRetryBackoffMs(123);
        config.setConnectTimeoutMs(4567);
        config.setAllocationLimit(1 << 20);
        return config;
    }

    @Test
    void readsTheExpiryOfServerSessions() {
        long expiry = System.currentTimeMillis() + 600_000;
        assertEquals(expiry, FlightSessionTokens.expiresAt(session(expiry)));
        String issued = KerberosTestAuthenticator.issue("alice@TEST.LOCAL", SECRET);
        long remaining = FlightSessionTokens.expiresAt(issued) - System.currentTimeMillis();
        assertTrue(remaining > 500_000 && remaining <= 600_000, "server sessions last ten minutes: " + remaining);
        assertEquals(0, FlightSessionTokens.expiresAt(null));
        assertEquals(0, FlightSessionTokens.expiresAt("no-separator"));
        assertEquals(0, FlightSessionTokens.expiresAt("!!!.sig"));
        assertEquals(0, FlightSessionTokens.expiresAt(Base64.getUrlEncoder().encodeToString(
                "2\n1\nx".getBytes(StandardCharsets.UTF_8)) + ".sig"));
    }

    @Test
    void findsOnlyUnexpiredSessionsOfTheRightKindAndServer() {
        long now = System.currentTimeMillis();
        String live = session(now + 600_000);
        Credentials creds = new Credentials();
        creds.addToken(FlightSessionTokens.alias("flight.test", 32010),
                FlightSessionTokens.token(live, "flight.test", 32010));
        assertEquals(live, FlightSessionTokens.find(creds, "flight.test", 32010, now));
        assertNull(FlightSessionTokens.find(creds, "flight.test", 32011, now), "other port");
        assertNull(FlightSessionTokens.find(creds, "other.test", 32010, now), "other host");
        assertNull(FlightSessionTokens.find(creds, "flight.test", 32010, now + 600_000), "expired");
        assertNull(FlightSessionTokens.find(creds, "flight.test", 32010,
                now + 600_000 - FlightSessionTokens.EXPIRY_MARGIN_MS), "about to expire");

        Credentials wrongKind = new Credentials();
        wrongKind.addToken(FlightSessionTokens.alias("flight.test", 32010), new Token<TokenIdentifier>(
                new byte[0], live.getBytes(StandardCharsets.UTF_8), new Text("HDFS_DELEGATION_TOKEN"),
                new Text("flight.test:32010")));
        assertNull(FlightSessionTokens.find(wrongKind, "flight.test", 32010, now));
    }

    @Test
    void keepsConfigurationsWithoutKerberosOrWithoutSession() throws Exception {
        Configuration password = new Configuration("flight.test", 32010, "u", "p", "");
        assertSame(password, FlightSessionTokens.resolve(password));
        Configuration kerberos = kerberos();
        UserGroupInformation executor = UserGroupInformation.createRemoteUser("executor");
        assertSame(kerberos, executor.doAs(
                (PrivilegedExceptionAction<Configuration>) () -> FlightSessionTokens.resolve(kerberos)));
    }

    @Test
    void usesTheSessionFromTheCurrentUsersCredentials() throws Exception {
        String live = session(System.currentTimeMillis() + 600_000);
        Credentials creds = new Credentials();
        creds.addToken(FlightSessionTokens.alias("flight.test", 32010),
                FlightSessionTokens.token(live, "flight.test", 32010));
        UserGroupInformation executor = UserGroupInformation.createRemoteUser("executor");
        executor.addCredentials(creds);
        Configuration kerberos = kerberos();

        Configuration resolved = executor.doAs(
                (PrivilegedExceptionAction<Configuration>) () -> FlightSessionTokens.resolve(kerberos));

        assertNull(resolved.getKerberosServicePrincipal());
        assertEquals(live, resolved.getBearerToken());
        assertEquals("", resolved.getPassword());
        assertEquals("flight.test", resolved.getFlightHost());
        assertEquals(32010, resolved.getFlightPort());
        assertTrue(resolved.getTlsEnabled());
        assertTrue(resolved.verifyServer());
        assertEquals("tpch", resolved.getDefaultSchema());
        assertEquals("tag", resolved.getRoutingTag());
        assertEquals("queue", resolved.getRoutingQueue());
        assertEquals(7, resolved.getMaxRetries());
        assertEquals(123, resolved.getRetryBackoffMs());
        assertEquals(4567, resolved.getConnectTimeoutMs());
        assertEquals(1 << 20, resolved.getAllocationLimit());
    }

    @Test
    void sessionTokenKeepsTheSessionAsPassword() {
        Token<TokenIdentifier> token = FlightSessionTokens.token("abc.def", "h", 1);
        assertEquals(FlightSessionTokens.KIND, token.getKind());
        assertEquals(new Text("h:1"), token.getService());
        assertArrayEquals("abc.def".getBytes(StandardCharsets.UTF_8), token.getPassword());
        assertEquals(0, token.getIdentifier().length);
    }
}
