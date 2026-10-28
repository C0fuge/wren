package pro.surpin.data.arrowflight.client.spark.security;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kerby.kerberos.kerb.server.SimpleKdcServer;

import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * An in-process KDC (Apache Kerby) of realm {@link #REALM} with keytabs for the client
 * {@code alice} and the Flight service {@code flight/localhost}, plus a password principal
 * {@code other/localhost}. Points {@code java.security.krb5.conf} at it and switches Hadoop's
 * {@link UserGroupInformation} to Kerberos; {@link #close()} restores both.
 */
final class KerberosKdcFixture implements AutoCloseable {
    static final String REALM = "EXAMPLE.COM";
    static final String CLIENT = "alice@" + REALM;
    static final String SERVICE = "flight/localhost@" + REALM;
    static final String OTHER_SERVICE = "other/localhost@" + REALM;

    private final SimpleKdcServer kdc;
    private final String previousKrb5Conf;
    private final Path clientKeytab;
    private final Path serviceKeytab;
    private final Path krb5Conf;

    /**
     * Starts the KDC on a free TCP port and writes keytabs and {@code krb5.conf} into a directory.
     *
     * @param dir working directory
     * @throws Exception on startup failure
     */
    KerberosKdcFixture(Path dir) throws Exception {
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
        clientKeytab = dir.resolve("alice.keytab");
        serviceKeytab = dir.resolve("flight.keytab");
        kdc.createAndExportPrincipals(clientKeytab.toFile(), "alice");
        kdc.createAndExportPrincipals(serviceKeytab.toFile(), "flight/localhost");
        kdc.createPrincipal("other/localhost", "other-password");
        krb5Conf = dir.resolve("krb5.conf");
        Files.writeString(krb5Conf, "[libdefaults]\n default_realm = " + REALM + "\n"
                + " udp_preference_limit = 1\n dns_lookup_kdc = false\n dns_lookup_realm = false\n"
                + " dns_canonicalize_hostname = false\n rdns = false\n"
                + "[realms]\n " + REALM + " = {\n  kdc = localhost:" + port + "\n }\n");
        previousKrb5Conf = System.setProperty("java.security.krb5.conf", krb5Conf.toString());
        UserGroupInformation.setConfiguration(hadoopConf());
    }

    /**
     * Hadoop settings of a Kerberos cluster in this realm.
     *
     * @return Hadoop configuration (no defaults loaded)
     */
    static org.apache.hadoop.conf.Configuration hadoopConf() {
        org.apache.hadoop.conf.Configuration hadoop = new org.apache.hadoop.conf.Configuration(false);
        hadoop.set("hadoop.security.authentication", "kerberos");
        hadoop.set("hadoop.security.auth_to_local", "RULE:[1:$1] RULE:[2:$1] DEFAULT");
        return hadoop;
    }

    /**
     * Logs the client in from its keytab (not as the process login user).
     *
     * @return client login
     * @throws Exception on login failure
     */
    UserGroupInformation loginClient() throws Exception {
        return UserGroupInformation.loginUserFromKeytabAndReturnUGI(CLIENT, clientKeytab.toString());
    }

    /**
     * Logs the Flight service in from its keytab (not as the process login user).
     *
     * @return service login
     * @throws Exception on login failure
     */
    UserGroupInformation loginService() throws Exception {
        return UserGroupInformation.loginUserFromKeytabAndReturnUGI(SERVICE, serviceKeytab.toString());
    }

    /**
     * Returns the client keytab.
     *
     * @return keytab of {@link #CLIENT}
     */
    Path clientKeytab() {
        return clientKeytab;
    }

    /**
     * Returns the Kerberos configuration file.
     *
     * @return {@code krb5.conf} pointing at this KDC
     */
    Path krb5Conf() {
        return krb5Conf;
    }

    @Override
    public void close() throws Exception {
        kdc.stop();
        UserGroupInformation.reset();
        UserGroupInformation.setConfiguration(new org.apache.hadoop.conf.Configuration(false));
        if (previousKrb5Conf == null) {
            System.clearProperty("java.security.krb5.conf");
        } else {
            System.setProperty("java.security.krb5.conf", previousKrb5Conf);
        }
    }
}
