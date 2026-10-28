package pro.surpin.data.arrowflight.client.spark.security;

import pro.surpin.data.arrowflight.client.Configuration;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.auth2.ClientIncomingAuthHeaderMiddleware;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkConf;
import org.apache.spark.security.HadoopDelegationTokenProvider;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Option;

import java.io.ByteArrayInputStream;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Spark delegation token provider (service {@value #SERVICE}) for Kerberos Flight servers.
 *
 * <p>On the driver, it authenticates to each configured server with SPNEGO under the current
 * Kerberos login and stores the signed session the server returns in the job credentials
 * ({@link FlightSessionTokens}); executors use it instead of their own SPNEGO handshake. Spark
 * re-obtains it before expiry (sessions last ten minutes) when the job runs with
 * {@code --principal/--keytab} or {@code spark.kerberos.renewal.credentials=ccache}.
 *
 * <p>Settings: {@code spark.flight.kerberos.servers} ({@code host:port} list, the host exactly as in
 * the read/write options), {@code spark.flight.kerberos.servicePrincipal},
 * {@code spark.flight.kerberos.tls.verifyServer} (default true),
 * {@code spark.flight.kerberos.tls.truststore.jksFile} and {@code .pass}. Spark's
 * {@code spark.security.credentials.flight.enabled=false} turns the provider off.
 */
public class FlightDelegationTokenProvider implements HadoopDelegationTokenProvider {
    /** Spark service name of this provider. */
    public static final String SERVICE = "flight";
    /** Prefix of the provider's Spark settings. */
    public static final String PREFIX = "spark.flight.kerberos.";
    /** Comma-separated {@code host:port} list. */
    public static final String SERVERS = PREFIX + "servers";
    /** Kerberos principal of the Flight service. */
    public static final String SERVICE_PRINCIPAL = PREFIX + "servicePrincipal";
    /** Whether to verify the server certificate (when no truststore is set). */
    public static final String TLS_VERIFY_SERVER = PREFIX + "tls.verifyServer";
    /** Truststore with the server certificate. */
    public static final String TLS_TRUSTSTORE_JKS = PREFIX + "tls.truststore.jksFile";
    /** Truststore password. */
    public static final String TLS_TRUSTSTORE_PASS = PREFIX + "tls.truststore.pass";

    private static final Logger LOGGER = LoggerFactory.getLogger(FlightDelegationTokenProvider.class);
    private static final String SPNEGO_OID = "1.3.6.1.5.5.2";
    private static final String KRB5_PRINCIPAL_OID = "1.2.840.113554.1.2.2.1";
    private static final String BEARER_PREFIX = "Bearer ";

    @Override
    public String serviceName() {
        return SERVICE;
    }

    @Override
    public boolean delegationTokensRequired(SparkConf sparkConf, org.apache.hadoop.conf.Configuration hadoopConf) {
        return !sparkConf.get(SERVERS, "").isBlank() && !sparkConf.get(SERVICE_PRINCIPAL, "").isBlank();
    }

    @Override
    public Option<Object> obtainDelegationTokens(org.apache.hadoop.conf.Configuration hadoopConf,
            SparkConf sparkConf, Credentials creds) {
        return obtain(sparkConf, creds, FlightDelegationTokenProvider::negotiate);
    }

    /**
     * Obtains a session from each configured server and adds it to the credentials.
     *
     * @param sparkConf Spark settings
     * @param creds job credentials
     * @param authorization handshake Authorization header for a service principal
     * @return earliest session expiry (Spark re-obtains before it), or empty without servers
     */
    static Option<Object> obtain(SparkConf sparkConf, Credentials creds, Function<String, String> authorization) {
        String principal = sparkConf.get(SERVICE_PRINCIPAL, "").trim();
        long next = Long.MAX_VALUE;
        for (String[] server : servers(sparkConf.get(SERVERS, ""))) {
            String host = server[0];
            int port = Integer.parseInt(server[1]);
            String session = handshake(location(host, port, sparkConf), () -> authorization.apply(principal));
            long expiresAt = FlightSessionTokens.expiresAt(session);
            if (expiresAt <= 0) {
                throw new IllegalStateException("Flight server " + host + ":" + port
                        + " issued an unreadable session");
            }
            creds.addToken(FlightSessionTokens.alias(host, port), FlightSessionTokens.token(session, host, port));
            next = Math.min(next, expiresAt);
            LOGGER.info("Obtained a Flight session for {}:{} (expires at {})", host, port, expiresAt);
        }
        return next == Long.MAX_VALUE ? Option.empty() : Option.apply(next);
    }

    /**
     * Parses the {@code host:port} list.
     *
     * @param value comma-separated list
     * @return host and port pairs
     */
    static List<String[]> servers(String value) {
        List<String[]> servers = new ArrayList<>();
        for (String item : value.split(",")) {
            String server = item.trim();
            if (server.isEmpty()) {
                continue;
            }
            int colon = server.lastIndexOf(':');
            if (colon <= 0 || colon == server.length() - 1) {
                throw new IllegalArgumentException(SERVERS + ": expected host:port, got '" + server + "'");
            }
            String port = server.substring(colon + 1);
            Integer.parseInt(port);
            servers.add(new String[] {server.substring(0, colon), port});
        }
        return servers;
    }

    /**
     * TLS endpoint settings of one server (Kerberos Flight authentication requires TLS).
     *
     * @param host Flight host
     * @param port Flight port
     * @param sparkConf Spark settings
     * @return endpoint
     */
    static Endpoint location(String host, int port, SparkConf sparkConf) {
        String jks = sparkConf.get(TLS_TRUSTSTORE_JKS, "");
        byte[] pem = null;
        if (!jks.isEmpty()) {
            pem = new Configuration(host, port, jks, sparkConf.get(TLS_TRUSTSTORE_PASS, ""), "", "", "")
                    .getCertificateBytes();
            if (pem == null || pem.length == 0) {
                throw new IllegalStateException(TLS_TRUSTSTORE_JKS + ": no certificate in " + jks);
            }
        }
        return new Endpoint(Location.forGrpcTls(host, port),
                Boolean.parseBoolean(sparkConf.get(TLS_VERIFY_SERVER, "true")), pem);
    }

    /**
     * Authenticates once and returns the bearer session the server sends back.
     *
     * @param endpoint server endpoint
     * @param authorization value of the handshake's Authorization header
     * @return signed bearer session
     */
    static String handshake(Endpoint endpoint, Supplier<String> authorization) {
        AtomicReference<String> issued = new AtomicReference<>();
        ClientIncomingAuthHeaderMiddleware.Factory capture = new ClientIncomingAuthHeaderMiddleware.Factory(
                headers -> {
                    String value = headers.get("Authorization");
                    if (value != null && value.startsWith(BEARER_PREFIX)) {
                        issued.set(value.substring(BEARER_PREFIX.length()));
                    }
                    return null;
                });
        try (BufferAllocator allocator = new RootAllocator()) {
            FlightClient.Builder builder = FlightClient.builder().allocator(allocator)
                    .location(endpoint.location()).intercept(capture);
            if (endpoint.location().getUri().getScheme().equals("grpc+tls")) {
                builder.useTls();
                if (endpoint.pem() != null) {
                    builder.trustedCertificates(new ByteArrayInputStream(endpoint.pem()));
                } else {
                    builder.verifyServer(endpoint.verifyServer());
                }
            }
            String header = authorization.get();
            try (FlightClient client = builder.build()) {
                client.handshake(new CredentialCallOption(headers -> insert(headers, header)));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted during the Flight handshake", e);
        }
        if (issued.get() == null) {
            throw new IllegalStateException("Flight server " + endpoint.location().getUri()
                    + " did not issue a session (is Kerberos authentication enabled on it?)");
        }
        return issued.get();
    }

    /**
     * Adds the Authorization header.
     *
     * @param headers outgoing headers
     * @param value header value
     */
    private static void insert(CallHeaders headers, String value) {
        headers.insert("Authorization", value);
    }

    /**
     * Creates a SPNEGO {@code Negotiate} header for the service with the current Kerberos login.
     *
     * @param servicePrincipal Kerberos principal of the Flight service
     * @return header value
     */
    static String negotiate(String servicePrincipal) {
        try {
            UserGroupInformation user = UserGroupInformation.getCurrentUser();
            PrivilegedExceptionAction<byte[]> ticket = () -> {
                GSSManager manager = GSSManager.getInstance();
                GSSName target = manager.createName(servicePrincipal, new Oid(KRB5_PRINCIPAL_OID));
                GSSContext context = manager.createContext(target, new Oid(SPNEGO_OID), null,
                        GSSContext.DEFAULT_LIFETIME);
                try {
                    context.requestMutualAuth(false);
                    return context.initSecContext(new byte[0], 0, 0);
                } finally {
                    context.dispose();
                }
            };
            byte[] token = user.hasKerberosCredentials() ? user.doAs(ticket) : ticket.run();
            if (token == null || token.length == 0) {
                throw new IllegalStateException("Kerberos did not produce a service ticket");
            }
            return "Negotiate " + Base64.getEncoder().encodeToString(token);
        } catch (Exception e) {
            throw new IllegalStateException("Kerberos login cannot authenticate to Flight service "
                    + servicePrincipal + " (no TGT on the driver?)", e);
        }
    }

    /**
     * Server endpoint for the handshake.
     *
     * @param location Flight location
     * @param verifyServer whether to verify the certificate (without a truststore)
     * @param pem trusted certificate (PEM), or null for the default trust
     */
    record Endpoint(Location location, boolean verifyServer, byte[] pem) {
    }
}
