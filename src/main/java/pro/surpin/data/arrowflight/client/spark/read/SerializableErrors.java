package pro.surpin.data.arrowflight.client.spark.read;

import org.apache.arrow.flight.FlightRuntimeException;

import java.io.IOException;
import java.io.Serial;

/**
 * Makes read and write errors safe to ship from an executor to the driver. Arrow's {@link FlightRuntimeException} holds a
 * {@code CallStatus}, which is not {@code Serializable}, so a task failing with it (directly or as a cause) cannot
 * be serialized: Spark then reports the failure without its exception, or, where its serialization debugger cannot
 * run (JVMs without spark-submit's {@code --add-opens}), the task thread dies and the job hangs.
 */
public final class SerializableErrors {

    private SerializableErrors() {
    }

    /**
     * Returns the error unchanged when its cause chain holds no {@link FlightRuntimeException}; otherwise the same
     * chain with every element from the first Flight exception on replaced by a {@link RemoteFlightError} that
     * keeps its class name, message (with the Flight status code) and stack trace.
     *
     * @param error error thrown by the Flight stream
     * @return an error whose cause chain is serializable as far as the Flight part goes
     */
    static Throwable serializable(Throwable error) {
        if (!hasFlightError(error)) {
            return error;
        }
        if (error instanceof FlightRuntimeException) {
            return copy(error);
        }
        IOException wrapped = new IOException(error.getMessage(), copy(error.getCause()));
        wrapped.setStackTrace(error.getStackTrace());
        return wrapped;
    }

    /**
     * Converts an error for {@code next()} of a Spark {@code PartitionReader}, which may only throw
     * {@link IOException} or unchecked exceptions.
     *
     * @param error error thrown by the Flight stream
     * @return the error to throw
     */
    public static IOException asIOException(IOException error) {
        Throwable converted = serializable(error);
        return converted instanceof IOException io ? io : new IOException(converted.getMessage(), converted);
    }

    /**
     * Converts an unchecked error thrown while opening or reading a stream, or while writing rows.
     *
     * @param error error thrown by the Flight stream
     * @return the error to throw
     */
    public static RuntimeException asRuntimeException(RuntimeException error) {
        if (!hasFlightError(error)) {
            return error;
        }
        Throwable converted = serializable(error);
        return converted instanceof RuntimeException runtime ? runtime
                : new IllegalStateException(converted.getMessage(), converted);
    }

    private static boolean hasFlightError(Throwable error) {
        for (Throwable t = error; t != null; t = t.getCause() == t ? null : t.getCause()) {
            if (t instanceof FlightRuntimeException) {
                return true;
            }
        }
        return false;
    }

    private static RemoteFlightError copy(Throwable error) {
        if (error == null) {
            return null;
        }
        Throwable cause = error.getCause() == error ? null : error.getCause();
        String status = error instanceof FlightRuntimeException flight ? flight.status().code() + ": " : "";
        RemoteFlightError copy = new RemoteFlightError(
                error.getClass().getName() + ": " + status + error.getMessage(), copy(cause));
        copy.setStackTrace(error.getStackTrace());
        return copy;
    }

    /**
     * Serializable stand-in for an error of the Flight stream: the original class name and message, its stack
     * trace and (as copies) its causes.
     */
    static final class RemoteFlightError extends RuntimeException {
        @Serial
        private static final long serialVersionUID = 1L;

        RemoteFlightError(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
