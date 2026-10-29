package pro.surpin.data.arrowflight.client.spark.read;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightRuntimeException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests of {@link SerializableErrors}: Flight errors in a cause chain become serializable copies.
 */
@Tag("unit")
class SerializableErrorsTest {

    private static FlightRuntimeException flightError() {
        return CallStatus.INVALID_ARGUMENT.withDescription("Unknown function: hash").toRuntimeException();
    }

    private static Object roundTrip(Object value) throws IOException, ClassNotFoundException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(value);
        }
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return in.readObject();
        }
    }

    /** Arrow's Flight exception itself cannot be serialized (what the conversion is for). */
    @Test
    void flightExceptionIsNotSerializable() {
        assertThrows(NotSerializableException.class, () -> roundTrip(new IOException("read", flightError())));
    }

    /** A wrapped Flight error keeps the outer message, the Flight class, status and message, and the stack. */
    @Test
    void wrappedFlightErrorBecomesSerializable() throws Exception {
        FlightRuntimeException flight = flightError();
        IOException original = new IOException("Error reading Arrow batch from Flight stream", flight);

        IOException converted = SerializableErrors.asIOException(original);
        assertEquals(original.getMessage(), converted.getMessage());
        assertArrayEquals(original.getStackTrace(), converted.getStackTrace());
        Throwable cause = converted.getCause();
        assertInstanceOf(SerializableErrors.RemoteFlightError.class, cause);
        assertTrue(cause.getMessage().startsWith(FlightRuntimeException.class.getName() + ": "), cause.getMessage());
        assertTrue(cause.getMessage().contains("INVALID_ARGUMENT"), cause.getMessage());
        assertTrue(cause.getMessage().contains("Unknown function: hash"), cause.getMessage());
        assertArrayEquals(flight.getStackTrace(), cause.getStackTrace());

        IOException back = (IOException) roundTrip(converted);
        assertEquals(cause.getMessage(), back.getCause().getMessage());
    }

    /** A Flight error thrown directly becomes a serializable runtime exception; its own causes are copied too. */
    @Test
    void directFlightErrorAndItsCausesAreCopied() throws Exception {
        FlightRuntimeException flight = CallStatus.UNAVAILABLE.withCause(new IllegalStateException("io"))
                .withDescription("down").toRuntimeException();
        RuntimeException converted = SerializableErrors.asRuntimeException(flight);
        assertInstanceOf(SerializableErrors.RemoteFlightError.class, converted);
        assertTrue(converted.getMessage().contains("down"), converted.getMessage());
        roundTrip(converted);

        RuntimeException nested = SerializableErrors.asRuntimeException(new IllegalStateException("outer", flight));
        assertInstanceOf(IllegalStateException.class, nested);
        roundTrip(nested);
    }

    /** Errors without a Flight exception are returned as they are. */
    @Test
    void otherErrorsAreUnchanged() {
        IOException io = new IOException("disk", new IllegalArgumentException("x"));
        assertSame(io, SerializableErrors.asIOException(io));
        IllegalStateException state = new IllegalStateException("state");
        assertSame(state, SerializableErrors.asRuntimeException(state));
    }
}
