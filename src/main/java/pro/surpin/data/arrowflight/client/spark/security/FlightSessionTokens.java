package pro.surpin.data.arrowflight.client.spark.security;

import pro.surpin.data.arrowflight.client.Client;
import pro.surpin.data.arrowflight.client.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Flight server sessions shipped in the Spark job credentials.
 *
 * <p>With Kerberos, the server answers a SPNEGO handshake with a signed bearer session (valid ten
 * minutes, accepted by every node sharing the session secret). On YARN, executors have no TGT, so
 * the driver obtains such a session ({@link FlightDelegationTokenProvider}) and Spark distributes it
 * as a Hadoop token; executors use it instead of their own SPNEGO handshake. When no usable session
 * is in the credentials, the configuration is used as is (SPNEGO with the current Kerberos login).
 */
public final class FlightSessionTokens {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlightSessionTokens.class);
    /** Hadoop token kind of a Flight session. */
    public static final Text KIND = new Text("FLIGHT_KERBEROS_SESSION");
    /** A session closer to expiry than this is not used (same margin as the client core). */
    static final long EXPIRY_MARGIN_MS = 30_000L;
    private static final String ALIAS_PREFIX = "flight-session:";
    private static final Map<String, String> TRUSTSTORES = new ConcurrentHashMap<>();
    private static final SecureRandom RANDOM = new SecureRandom();

    /** Utility class. */
    private FlightSessionTokens() {
    }

    /**
     * Returns the credentials alias of the session for one server.
     *
     * @param host Flight host as configured in the job
     * @param port Flight port
     * @return credentials alias
     */
    public static Text alias(String host, int port) {
        return new Text(ALIAS_PREFIX + host + ":" + port);
    }

    /**
     * Wraps a server session as a Hadoop token.
     *
     * @param session signed bearer session issued by the server
     * @param host Flight host
     * @param port Flight port
     * @return token for the job credentials
     */
    public static Token<TokenIdentifier> token(String session, String host, int port) {
        return new Token<>(new byte[0], session.getBytes(StandardCharsets.UTF_8), KIND,
                new Text(host + ":" + port));
    }

    /**
     * Reads the expiry of a server session (payload {@code 1\n<expiry ms>\n<principal>}).
     *
     * @param session signed bearer session
     * @return expiry in Unix milliseconds, or 0 when the session cannot be parsed
     */
    public static long expiresAt(String session) {
        if (session == null) {
            return 0;
        }
        int separator = session.indexOf('.');
        if (separator <= 0) {
            return 0;
        }
        try {
            String payload = new String(Base64.getUrlDecoder().decode(session.substring(0, separator)),
                    StandardCharsets.UTF_8);
            String[] fields = payload.split("\n", 3);
            return fields.length == 3 && "1".equals(fields[0]) ? Long.parseLong(fields[1]) : 0;
        } catch (IllegalArgumentException e) {
            return 0;
        }
    }

    /**
     * Finds a usable session for a server in the current user's (else the login user's) credentials.
     *
     * @param host Flight host
     * @param port Flight port
     * @param now current time in Unix milliseconds
     * @return the session, or null when none is present or it is about to expire
     */
    static String find(String host, int port, long now) {
        try {
            String session = find(UserGroupInformation.getCurrentUser().getCredentials(), host, port, now);
            return session != null ? session
                    : find(UserGroupInformation.getLoginUser().getCredentials(), host, port, now);
        } catch (IOException e) {
            LOGGER.warn("Cannot read the Hadoop credentials for a Flight session", e);
            return null;
        }
    }

    /**
     * Finds a usable session for a server in the given credentials.
     *
     * @param credentials Hadoop credentials
     * @param host Flight host
     * @param port Flight port
     * @param now current time in Unix milliseconds
     * @return the session, or null when none is present or it is about to expire
     */
    static String find(Credentials credentials, String host, int port, long now) {
        Token<? extends TokenIdentifier> token = credentials.getToken(alias(host, port));
        if (token == null || !KIND.equals(token.getKind())) {
            return null;
        }
        String session = new String(token.getPassword(), StandardCharsets.UTF_8);
        return expiresAt(session) > now + EXPIRY_MARGIN_MS ? session : null;
    }

    /**
     * Returns the configuration to connect with: for a Kerberos configuration with a usable session
     * in the job credentials, a copy that authenticates with that session; otherwise the
     * configuration itself.
     *
     * @param config connector configuration
     * @return configuration to pass to {@link Client#getOrCreate(Configuration)}
     */
    public static Configuration resolve(Configuration config) {
        if (config.getKerberosServicePrincipal() == null) {
            return config;
        }
        String session = find(config.getFlightHost(), config.getFlightPort(), System.currentTimeMillis());
        return session == null ? config : withSession(config, session);
    }

    /**
     * Connects with {@link #resolve(Configuration)}; a failing Kerberos connection without a session
     * in the credentials is reported with what is missing.
     *
     * @param config connector configuration
     * @return pooled client
     */
    public static Client connect(Configuration config) {
        Configuration effective = resolve(config);
        if (effective != config) {
            return Client.getOrCreate(effective);
        }
        try {
            return Client.getOrCreate(config);
        } catch (RuntimeException e) {
            if (config.getKerberosServicePrincipal() == null) {
                throw e;
            }
            throw new IllegalStateException("Kerberos Flight connection to " + config.getFlightHost() + ":"
                    + config.getFlightPort() + " failed. No Flight session for this server was in the job "
                    + "credentials (set spark.flight.kerberos.servers and .servicePrincipal for the driver; long "
                    + "jobs need --principal/--keytab), so this process's own Kerberos login was used: " + e, e);
        }
    }

    /**
     * Copies a Kerberos configuration so that it authenticates with a server session instead.
     *
     * @param config Kerberos configuration
     * @param session signed bearer session
     * @return bearer configuration with the same endpoint, trust and request settings
     */
    static Configuration withSession(Configuration config, String session) {
        String user = config.getUser() == null ? "" : config.getUser();
        Configuration copy;
        if (config.getTruststoreJks() != null && !config.getTruststoreJks().isEmpty()) {
            byte[] pem = config.getCertificateBytes();
            if (pem == null || pem.length == 0) {
                return config;
            }
            String[] store = localTruststore(pem).split("\n", 2);
            copy = new Configuration(config.getFlightHost(), config.getFlightPort(), store[0], store[1], user,
                    "", session);
        } else {
            copy = new Configuration(config.getFlightHost(), config.getFlightPort(), config.getTlsEnabled(),
                    config.verifyServer(), user, "", session);
        }
        copy.setDefaultSchema(config.getDefaultSchema());
        copy.setRoutingTag(config.getRoutingTag());
        copy.setRoutingQueue(config.getRoutingQueue());
        copy.setMaxRetries(config.getMaxRetries());
        copy.setRetryBackoffMs(config.getRetryBackoffMs());
        copy.setConnectTimeoutMs(config.getConnectTimeoutMs());
        copy.setAllocationLimit(config.getAllocationLimit());
        return copy;
    }

    /**
     * Writes the driver's trusted certificate (PEM, read from the driver's truststore) to a private
     * truststore in this process: the truststore path of the job need not exist on executors.
     *
     * @param pem PEM certificate bytes
     * @return path and password of the truststore, separated by a newline
     */
    private static String localTruststore(byte[] pem) {
        return TRUSTSTORES.computeIfAbsent(sha256(pem), key -> {
            try {
                Certificate certificate = CertificateFactory.getInstance("X.509")
                        .generateCertificate(new ByteArrayInputStream(pem));
                byte[] random = new byte[18];
                RANDOM.nextBytes(random);
                String password = Base64.getUrlEncoder().withoutPadding().encodeToString(random);
                KeyStore store = KeyStore.getInstance("PKCS12");
                store.load(null, null);
                store.setCertificateEntry("flight", certificate);
                Path file = Files.createTempFile("flight-truststore-", ".p12",
                        PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
                file.toFile().deleteOnExit();
                try (OutputStream out = Files.newOutputStream(file)) {
                    store.store(out, password.toCharArray());
                }
                return file + "\n" + password;
            } catch (GeneralSecurityException | IOException e) {
                throw new IllegalStateException("Cannot write the Flight truststore for the job session", e);
            }
        });
    }

    /**
     * Hex SHA-256 of bytes.
     *
     * @param bytes input
     * @return hex digest
     */
    private static String sha256(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }
}
