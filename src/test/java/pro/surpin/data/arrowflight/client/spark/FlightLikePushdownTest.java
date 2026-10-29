package pro.surpin.data.arrowflight.client.spark;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests the conversion of Spark {@code LIKE} patterns for the server.
 */
@Tag("unit")
class FlightLikePushdownTest {

    /** Literal characters and {@code %} wildcards keep their meaning; server wildcards in literals are escaped. */
    @Test
    void convertsPercentPatterns() {
        assertEquals("%special%requests%", FlightLikePushdown.sqlPattern("%special%requests%", '\\'));
        assertEquals("a%b", FlightLikePushdown.sqlPattern("a%b", '\\'));
        assertEquals("abc", FlightLikePushdown.sqlPattern("abc", '\\'));
        assertEquals("", FlightLikePushdown.sqlPattern("", '\\'));
        assertEquals("50\\%%off", FlightLikePushdown.sqlPattern("50\\%%off", '\\'));
        assertEquals("a\\_b%", FlightLikePushdown.sqlPattern("a\\_b%", '\\'));
        assertEquals("c:\\\\%", FlightLikePushdown.sqlPattern("c:\\\\%", '\\'));
        assertEquals("it's%", FlightLikePushdown.sqlPattern("it's%", '\\'));
    }

    /** Another escape character: a backslash is a literal and must be escaped for the server. */
    @Test
    void convertsOtherEscapeCharacters() {
        assertEquals("a\\\\%b\\%", FlightLikePushdown.sqlPattern("a\\%b!%", '!'));
        assertEquals("!%", FlightLikePushdown.sqlPattern("!!%", '!'));
        assertEquals("\\%", FlightLikePushdown.sqlPattern("%%", '%'));
    }

    /** {@code _} wildcards and escapes Spark rejects are not sent. */
    @Test
    void refusesUnderscoresAndInvalidEscapes() {
        assertNull(FlightLikePushdown.sqlPattern("%a_b%", '\\'));
        assertNull(FlightLikePushdown.sqlPattern("abc\\", '\\'));
        assertNull(FlightLikePushdown.sqlPattern("a\\bc", '\\'));
    }
}
