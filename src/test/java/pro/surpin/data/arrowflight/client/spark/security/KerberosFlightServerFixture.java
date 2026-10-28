package pro.surpin.data.arrowflight.client.spark.security;

import pro.surpin.data.arrowflight.server.KerberosTestAuthenticator;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

import java.io.ByteArrayInputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.Date;

/**
 * A TLS Flight server (self-signed certificate for localhost) that authenticates with the server's
 * Kerberos authenticator: by default only {@link #TICKET} is accepted as a SPNEGO ticket.
 */
final class KerberosFlightServerFixture implements AutoCloseable {
    static final byte[] TICKET = "test-spnego-ticket".getBytes(StandardCharsets.UTF_8);
    static final String PRINCIPAL = "alice@TEST.LOCAL";
    static final String SERVICE_PRINCIPAL = "flight/localhost@TEST.LOCAL";
    static final byte[] SECRET = new byte[32];

    private final BufferAllocator allocator = new RootAllocator();
    private final FlightServer server;
    private final X509Certificate certificate;

    /**
     * Starts the server on a free port with the stub ticket check.
     *
     * @throws Exception on startup failure
     */
    KerberosFlightServerFixture() throws Exception {
        this(KerberosTestAuthenticator.accepting(TICKET, PRINCIPAL, SECRET));
    }

    /**
     * Starts the server on a free port with the given authenticator.
     *
     * @param authenticator Flight header authenticator
     * @throws Exception on startup failure
     */
    KerberosFlightServerFixture(CallHeaderAuthenticator authenticator) throws Exception {
        this(authenticator, new NoOpFlightProducer());
    }

    /**
     * Starts the server on a free port with the given authenticator in front of a producer.
     *
     * @param authenticator Flight header authenticator
     * @param producer Flight producer serving the calls
     * @throws Exception on startup failure
     */
    KerberosFlightServerFixture(CallHeaderAuthenticator authenticator, FlightProducer producer) throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        KeyPair keys = generator.generateKeyPair();
        X500Name name = new X500Name("CN=localhost");
        long now = System.currentTimeMillis();
        JcaX509v3CertificateBuilder builder = new JcaX509v3CertificateBuilder(name, BigInteger.valueOf(now),
                new Date(now - 60_000), new Date(now + 86_400_000), name, keys.getPublic());
        builder.addExtension(Extension.subjectAlternativeName, false,
                new GeneralNames(new GeneralName(GeneralName.dNSName, "localhost")));
        certificate = new JcaX509CertificateConverter().getCertificate(
                builder.build(new JcaContentSignerBuilder("SHA256withRSA").build(keys.getPrivate())));
        byte[] key = pem("PRIVATE KEY", keys.getPrivate().getEncoded());
        server = FlightServer.builder(allocator, Location.forGrpcTls("localhost", 0), producer)
                .useTls(new ByteArrayInputStream(certificatePem()), new ByteArrayInputStream(key))
                .headerAuthenticator(authenticator)
                .build()
                .start();
    }

    /**
     * Returns the port.
     *
     * @return listening port
     */
    int port() {
        return server.getPort();
    }

    /**
     * Returns the server certificate as PEM.
     *
     * @return PEM bytes
     * @throws Exception on encoding failure
     */
    byte[] certificatePem() throws Exception {
        return pem("CERTIFICATE", certificate.getEncoded());
    }

    /**
     * Writes a truststore with the server certificate.
     *
     * @param file target file
     * @param password store password
     * @throws Exception on write failure
     */
    void writeTruststore(Path file, String password) throws Exception {
        KeyStore store = KeyStore.getInstance("PKCS12");
        store.load(null, null);
        store.setCertificateEntry("server", certificate);
        try (OutputStream out = Files.newOutputStream(file)) {
            store.store(out, password.toCharArray());
        }
    }

    /**
     * Returns the SPNEGO header the server accepts.
     *
     * @return Authorization header value
     */
    static String negotiate() {
        return "Negotiate " + Base64.getEncoder().encodeToString(TICKET);
    }

    /**
     * PEM-encodes DER bytes.
     *
     * @param type PEM type
     * @param der DER bytes
     * @return PEM bytes
     */
    private static byte[] pem(String type, byte[] der) {
        String body = Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII)).encodeToString(der);
        return ("-----BEGIN " + type + "-----\n" + body + "\n-----END " + type + "-----\n")
                .getBytes(StandardCharsets.US_ASCII);
    }

    @Override
    public void close() throws Exception {
        server.close();
        allocator.close();
    }
}
