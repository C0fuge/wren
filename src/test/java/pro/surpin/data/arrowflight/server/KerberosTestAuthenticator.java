package pro.surpin.data.arrowflight.server;

import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;

import java.time.Clock;
import java.util.Arrays;

/**
 * The server's Kerberos authenticator and session codec with a stub ticket check, for connector
 * tests without a KDC: one fixed SPNEGO ticket is accepted for one principal.
 */
public final class KerberosTestAuthenticator {

    /** Utility class. */
    private KerberosTestAuthenticator() {
    }

    /**
     * Creates the server authenticator.
     *
     * @param ticket the only accepted SPNEGO ticket
     * @param principal principal the ticket authenticates
     * @param secret session HMAC key (at least 32 bytes)
     * @return Flight header authenticator issuing signed sessions
     */
    public static CallHeaderAuthenticator accepting(byte[] ticket, String principal, byte[] secret) {
        byte[] expected = ticket.clone();
        return new KerberosFlightAuthenticator(received -> {
            if (!Arrays.equals(received, expected)) {
                throw new IllegalArgumentException("Unknown ticket");
            }
            return principal;
        }, new KerberosSessionToken(secret, Clock.systemUTC()));
    }

    /**
     * Issues a session as a server with this secret would.
     *
     * @param principal session principal
     * @param secret session HMAC key
     * @return signed session
     */
    public static String issue(String principal, byte[] secret) {
        return new KerberosSessionToken(secret, Clock.systemUTC()).issue(principal);
    }
}
