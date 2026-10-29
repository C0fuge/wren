package pro.surpin.data.arrowflight.client.spark.read;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies zero-copy scalar access from Flight-owned Arrow vectors.
 */
@Tag("unit")
class FlightArrowColumnVectorTest {

    /** Verifies UTF-8 values remain views over a standard Arrow string buffer. */
    @Test
    void readsVarCharWithoutCopyingDataBuffer() {
        try (RootAllocator allocator = new RootAllocator();
                VarCharVector vector = new VarCharVector("value", allocator)) {
            vector.allocateNew();
            vector.setSafe(0, "hello".getBytes(StandardCharsets.UTF_8));
            vector.setNull(1);
            vector.setValueCount(2);
            FlightArrowColumnVector column = new FlightArrowColumnVector(vector);

            UTF8String value = column.getUTF8String(0);
            vector.getDataBuffer().setByte(0, 'H');

            assertEquals("Hello", value.toString());
            assertNull(column.getUTF8String(1));
        }
    }

    /** Verifies UTF-8 values remain views over a large Arrow string buffer. */
    @Test
    void readsLargeVarCharWithoutCopyingDataBuffer() {
        try (RootAllocator allocator = new RootAllocator();
                LargeVarCharVector vector = new LargeVarCharVector("value", allocator)) {
            vector.allocateNew();
            vector.setSafe(0, "flight".getBytes(StandardCharsets.UTF_8));
            vector.setValueCount(1);
            FlightArrowColumnVector column = new FlightArrowColumnVector(vector);

            UTF8String value = column.getUTF8String(0);
            vector.getDataBuffer().setByte(0, 'F');

            assertEquals("Flight", value.toString());
        }
    }

    /** Verifies long-sized decimals (Spark's compact form) equal the BigDecimal path, signs and bounds included. */
    @Test
    void readsCompactDecimalsAsLongs() {
        try (RootAllocator allocator = new RootAllocator();
                DecimalVector vector = new DecimalVector("value", allocator, 15, 2)) {
            BigDecimal[] values = {
                new BigDecimal("12345.67"), new BigDecimal("-12345.67"), BigDecimal.ZERO.setScale(2),
                new BigDecimal("9999999999999.99"), new BigDecimal("-9999999999999.99"), new BigDecimal("-0.01")
            };
            vector.allocateNew(values.length + 1);
            for (int i = 0; i < values.length; i++) {
                vector.setSafe(i, values[i]);
            }
            vector.setNull(values.length);
            vector.setValueCount(values.length + 1);
            FlightArrowColumnVector column = new FlightArrowColumnVector(vector);

            for (int i = 0; i < values.length; i++) {
                Decimal decimal = column.getDecimal(i, 15, 2);
                assertEquals(values[i], decimal.toJavaBigDecimal());
                assertEquals(Decimal.apply(values[i], 15, 2), decimal);
                assertTrue(decimal.precision() == 15 && decimal.scale() == 2);
            }
            assertNull(column.getDecimal(values.length, 15, 2));
        }
    }

    /** Verifies values beyond 64 bits or beyond the declared precision take the BigDecimal path (and its checks). */
    @Test
    void wideOrOutOfPrecisionDecimalsUseTheBigDecimalPath() {
        try (RootAllocator allocator = new RootAllocator();
                DecimalVector wide = new DecimalVector("wide", allocator, 38, 0);
                DecimalVector narrow = new DecimalVector("narrow", allocator, 4, 0)) {
            BigDecimal big = new BigDecimal(BigInteger.ONE.shiftLeft(100).negate());
            wide.allocateNew(1);
            wide.setSafe(0, big);
            wide.setValueCount(1);
            assertEquals(big, new FlightArrowColumnVector(wide).getDecimal(0, 38, 0).toJavaBigDecimal());

            // An unscaled value outside the declared precision is written below the vector's own checks.
            narrow.allocateNew(1);
            narrow.getDataBuffer().setLong(0, 123_456L);
            narrow.getDataBuffer().setLong(Long.BYTES, 0L);
            narrow.setIndexDefined(0);
            narrow.setValueCount(1);
            FlightArrowColumnVector column = new FlightArrowColumnVector(narrow);
            assertThrows(ArithmeticException.class, () -> column.getDecimal(0, 4, 0));
        }
    }
}
