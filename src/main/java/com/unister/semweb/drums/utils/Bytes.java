package com.unister.semweb.drums.utils;

import java.lang.reflect.Field;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.misc.Unsafe;

/**
 * Utility class that handles byte arrays.
 */
@SuppressWarnings("restriction")
public class Bytes {
    private static final Logger LOGGER = LoggerFactory.getLogger(Bytes.class);
    /** Size of boolean in bytes */
    public static final int SIZEOF_BOOLEAN = Byte.SIZE / Byte.SIZE;

    /** Size of byte in bytes */
    public static final int SIZEOF_BYTE = SIZEOF_BOOLEAN;

    /** Size of char in bytes */
    public static final int SIZEOF_CHAR = Character.SIZE / Byte.SIZE;

    /** Size of double in bytes */
    public static final int SIZEOF_DOUBLE = Double.SIZE / Byte.SIZE;

    /** Size of float in bytes */
    public static final int SIZEOF_FLOAT = Float.SIZE / Byte.SIZE;

    /** Size of int in bytes */
    public static final int SIZEOF_INT = Integer.SIZE / Byte.SIZE;

    /** Size of long in bytes */
    public static final int SIZEOF_LONG = Long.SIZE / Byte.SIZE;

    /** Size of short in bytes */
    public static final int SIZEOF_SHORT = Short.SIZE / Byte.SIZE;

    /**
     * Estimate of size cost to pay beyond payload in jvm for instance of byte [].
     * Estimate based on study of jhat and jprofiler numbers.
     */
    // JHat says BU is 56 bytes.
    // SizeOf which uses java.lang.instrument says 24 bytes. (3 longs?)
    public static final int ESTIMATED_HEAP_TAX = 16;

    /**
     * Put bytes at the specified byte array position.
     * 
     * @param tgtBytes
     *            the byte array
     * @param tgtOffset
     *            position in the array
     * @param srcBytes
     *            array to write out
     * @param srcOffset
     *            source offset
     * @param srcLength
     *            source length
     * @return incremented offset
     */
    public static int putBytes(byte[] tgtBytes, int tgtOffset, byte[] srcBytes,
            int srcOffset, int srcLength) {
        System.arraycopy(srcBytes, srcOffset, tgtBytes, tgtOffset, srcLength);
        return tgtOffset + srcLength;
    }

    /**
     * Write a single byte out to the specified byte array position.
     * 
     * @param bytes
     *            the byte array
     * @param offset
     *            position in the array
     * @param b
     *            byte to write out
     * @return incremented offset
     */
    public static int putByte(byte[] bytes, int offset, byte b) {
        bytes[offset] = b;
        return offset + 1;
    }

    /**
     * Returns a new byte array, copied from the passed ByteBuffer.
     * 
     * @param bb
     *            A ByteBuffer
     * @return the byte array
     */
    public static byte[] toBytes(ByteBuffer bb) {
        int length = bb.limit();
        byte[] result = new byte[length];
        System.arraycopy(bb.array(), bb.arrayOffset(), result, 0, length);
        return result;
    }

    /**
     * Convert a boolean to a byte array. True becomes -1
     * and false becomes 0.
     * 
     * @param b
     *            value
     * @return <code>b</code> encoded in a byte array.
     */
    public static byte[] toBytes(final boolean b) {
        return new byte[] { b ? (byte) -1 : (byte) 0 };
    }

    /**
     * Reverses {@link #toBytes(boolean)}
     * 
     * @param b
     *            array
     * @return True or false.
     */
    public static boolean toBoolean(final byte[] b) {
        if (b.length != 1) {
            throw new IllegalArgumentException("Array has wrong size: " + b.length);
        }
        return b[0] != (byte) 0;
    }

    /**
     * Convert a long value to a byte array using big-endian.
     * 
     * @param val
     *            value to convert
     * @return the byte array
     */
    public static byte[] toBytes(long val) {
        byte[] b = new byte[8];
        for (int i = 7; i > 0; i--) {
            b[i] = (byte) val;
            val >>>= 8;
        }
        b[0] = (byte) val;
        return b;
    }

    /**
     * Converts a byte array to a long value. Reverses {@link #toBytes(long)}
     * 
     * @param bytes
     *            array
     * @return the long value
     */
    public static long toLong(byte[] bytes) {
        return toLong(bytes, 0, SIZEOF_LONG);
    }

    /**
     * Converts a byte array to a long value. Assumes there will be {@link #SIZEOF_LONG} bytes available.
     * 
     * @param bytes
     *            bytes
     * @param offset
     *            offset
     * @return the long value
     */
    public static long toLong(byte[] bytes, int offset) {
        return toLong(bytes, offset, SIZEOF_LONG);
    }

    /**
     * Converts a byte array to a long value.
     * 
     * @param bytes
     *            array of bytes
     * @param offset
     *            offset into array
     * @param length
     *            length of data (must be {@link #SIZEOF_LONG})
     * @return the long value
     * @throws IllegalArgumentException
     *             if length is not {@link #SIZEOF_LONG} or
     *             if there's not enough room in the array at the offset indicated.
     */
    public static long toLong(byte[] bytes, int offset, final int length) {
        if (length != SIZEOF_LONG || offset + length > bytes.length) {
            throw explainWrongLengthOrOffset(bytes, offset, length, SIZEOF_LONG);
        }
        long l = 0;
        for (int i = offset; i < offset + length; i++) {
            l <<= 8;
            l ^= bytes[i] & 0xFF;
        }
        return l;
    }

    private static IllegalArgumentException
            explainWrongLengthOrOffset(final byte[] bytes,
                    final int offset,
                    final int length,
                    final int expectedLength) {
        String reason;
        if (length != expectedLength) {
            reason = "Wrong length: " + length + ", expected " + expectedLength;
        } else {
            reason = "offset (" + offset + ") + length (" + length + ") exceed the"
                    + " capacity of the array: " + bytes.length;
        }
        return new IllegalArgumentException(reason);
    }

    /**
     * Put a long value out to the specified byte array position.
     * 
     * @param bytes
     *            the byte array
     * @param offset
     *            position in the array
     * @param val
     *            long to write out
     * @return incremented offset
     * @throws IllegalArgumentException
     *             if the byte array given doesn't have
     *             enough room at the offset specified.
     */
    public static int putLong(byte[] bytes, int offset, long val) {
        if (bytes.length - offset < SIZEOF_LONG) {
            throw new IllegalArgumentException("Not enough room to put a long at"
                    + " offset " + offset + " in a " + bytes.length + " byte array");
        }
        for (int i = offset + 7; i > offset; i--) {
            bytes[i] = (byte) val;
            val >>>= 8;
        }
        bytes[offset] = (byte) val;
        return offset + SIZEOF_LONG;
    }

    /**
     * Presumes float encoded as IEEE 754 floating-point "single format"
     * 
     * @param bytes
     *            byte array
     * @return Float made from passed byte array.
     */
    public static float toFloat(byte[] bytes) {
        return toFloat(bytes, 0);
    }

    /**
     * Presumes float encoded as IEEE 754 floating-point "single format"
     * 
     * @param bytes
     *            array to convert
     * @param offset
     *            offset into array
     * @return Float made from passed byte array.
     */
    public static float toFloat(byte[] bytes, int offset) {
        return Float.intBitsToFloat(toInt(bytes, offset, SIZEOF_INT));
    }

    /**
     * @param bytes
     *            byte array
     * @param offset
     *            offset to write to
     * @param f
     *            float value
     * @return New offset in <code>bytes</code>
     */
    public static int putFloat(byte[] bytes, int offset, float f) {
        return putInt(bytes, offset, Float.floatToRawIntBits(f));
    }

    /**
     * @param f
     *            float value
     * @return the float represented as byte []
     */
    public static byte[] toBytes(final float f) {
        // Encode it as int
        return Bytes.toBytes(Float.floatToRawIntBits(f));
    }

    /**
     * @param bytes
     *            byte array
     * @return Return double made from passed bytes.
     */
    public static double toDouble(final byte[] bytes) {
        return toDouble(bytes, 0);
    }

    /**
     * @param bytes
     *            byte array
     * @param offset
     *            offset where double is
     * @return Return double made from passed bytes.
     */
    public static double toDouble(final byte[] bytes, final int offset) {
        return Double.longBitsToDouble(toLong(bytes, offset, SIZEOF_LONG));
    }

    /**
     * @param bytes
     *            byte array
     * @param offset
     *            offset to write to
     * @param d
     *            value
     * @return New offset into array <code>bytes</code>
     */
    public static int putDouble(byte[] bytes, int offset, double d) {
        return putLong(bytes, offset, Double.doubleToLongBits(d));
    }

    /**
     * Serialize a double as the IEEE 754 double format output. The resultant
     * array will be 8 bytes long.
     * 
     * @param d
     *            value
     * @return the double represented as byte []
     */
    public static byte[] toBytes(final double d) {
        // Encode it as a long
        return Bytes.toBytes(Double.doubleToRawLongBits(d));
    }

    /**
     * Convert an int value to a byte array
     * 
     * @param val
     *            value
     * @return the byte array
     */
    public static byte[] toBytes(int val) {
        byte[] b = new byte[4];
        for (int i = 3; i > 0; i--) {
            b[i] = (byte) val;
            val >>>= 8;
        }
        b[0] = (byte) val;
        return b;
    }

    /**
     * Converts a byte array to an int value
     * 
     * @param bytes
     *            byte array
     * @return the int value
     */
    public static int toInt(byte[] bytes) {
        return toInt(bytes, 0, SIZEOF_INT);
    }

    /**
     * Converts a byte array to an int value
     * 
     * @param bytes
     *            byte array
     * @param offset
     *            offset into array
     * @return the int value
     */
    public static int toInt(byte[] bytes, int offset) {
        return toInt(bytes, offset, SIZEOF_INT);
    }

    /**
     * Converts a byte array to an int value
     * 
     * @param bytes
     *            byte array
     * @param offset
     *            offset into array
     * @param length
     *            length of int (has to be {@link #SIZEOF_INT})
     * @return the int value
     * @throws IllegalArgumentException
     *             if length is not {@link #SIZEOF_INT} or
     *             if there's not enough room in the array at the offset indicated.
     */
    public static int toInt(byte[] bytes, int offset, final int length) {
        if (length != SIZEOF_INT || offset + length > bytes.length) {
            throw explainWrongLengthOrOffset(bytes, offset, length, SIZEOF_INT);
        }
        int n = 0;
        for (int i = offset; i < (offset + length); i++) {
            n <<= 8;
            n ^= bytes[i] & 0xFF;
        }
        return n;
    }

    /**
     * Put an int value out to the specified byte array position.
     * 
     * @param bytes
     *            the byte array
     * @param offset
     *            position in the array
     * @param val
     *            int to write out
     * @return incremented offset
     * @throws IllegalArgumentException
     *             if the byte array given doesn't have
     *             enough room at the offset specified.
     */
    public static int putInt(byte[] bytes, int offset, int val) {
        if (bytes.length - offset < SIZEOF_INT) {
            throw new IllegalArgumentException("Not enough room to put an int at"
                    + " offset " + offset + " in a " + bytes.length + " byte array");
        }
        for (int i = offset + 3; i > offset; i--) {
            bytes[i] = (byte) val;
            val >>>= 8;
        }
        bytes[offset] = (byte) val;
        return offset + SIZEOF_INT;
    }

    /**
     * Convert a short value to a byte array of {@link #SIZEOF_SHORT} bytes long.
     * 
     * @param val
     *            value
     * @return the byte array
     */
    public static byte[] toBytes(short val) {
        byte[] b = new byte[SIZEOF_SHORT];
        b[1] = (byte) val;
        val >>= 8;
        b[0] = (byte) val;
        return b;
    }

    /**
     * Converts a byte array to a char value
     * 
     * @param bytes
     *            byte array
     * @return the char value
     */
    public static char toChar(byte[] bytes) {
        return toChar(bytes, 0, SIZEOF_CHAR);
    }

    /**
     * Converts a byte array to a char value
     * 
     * @param bytes
     *            byte array
     * @param offset
     *            offset into array
     * @return the char value
     */
    public static char toChar(byte[] bytes, int offset) {
        return toChar(bytes, offset, SIZEOF_CHAR);
    }

    /**
     * Converts a byte array to a char value
     * 
     * @param bytes
     *            byte array
     * @param offset
     *            offset into array
     * @param length
     *            length, has to be {@link #SIZEOF_CHAR}
     * @return the char value
     * @throws IllegalArgumentException
     *             if length is not {@link #SIZEOF_CHAR} or if there's not enough room in the array at the offset
     *             indicated.
     */
    public static char toChar(byte[] bytes, int offset, final int length) {
        if (length != SIZEOF_CHAR || offset + length > bytes.length) {
            throw explainWrongLengthOrOffset(bytes, offset, length, SIZEOF_CHAR);
        }
        char n = 0;
        n ^= bytes[offset] & 0xFF;
        n <<= 8;
        n ^= bytes[offset + 1] & 0xFF;
        return n;
    }

    /**
     * Converts a byte array to a short value
     * 
     * @param bytes
     *            byte array
     * @return the short value
     */
    public static short toShort(byte[] bytes) {
        return toShort(bytes, 0, SIZEOF_SHORT);
    }

    /**
     * Converts a byte array to a short value
     * 
     * @param bytes
     *            byte array
     * @param offset
     *            offset into array
     * @return the short value
     */
    public static short toShort(byte[] bytes, int offset) {
        return toShort(bytes, offset, SIZEOF_SHORT);
    }

    /**
     * Converts a byte array to a short value
     * 
     * @param bytes
     *            byte array
     * @param offset
     *            offset into array
     * @param length
     *            length, has to be {@link #SIZEOF_SHORT}
     * @return the short value
     * @throws IllegalArgumentException
     *             if length is not {@link #SIZEOF_SHORT} or if there's not enough room in the array at the offset
     *             indicated.
     */
    public static short toShort(byte[] bytes, int offset, final int length) {
        if (length != SIZEOF_SHORT || offset + length > bytes.length) {
            throw explainWrongLengthOrOffset(bytes, offset, length, SIZEOF_SHORT);
        }
        short n = 0;
        n ^= bytes[offset] & 0xFF;
        n <<= 8;
        n ^= bytes[offset + 1] & 0xFF;
        return n;
    }

    /**
     * This method will get a sequence of bytes from pos -> limit,
     * but will restore pos after.
     * 
     * @param buf
     * @return byte array
     */
    public static byte[] getBytes(ByteBuffer buf) {
        int savedPos = buf.position();
        byte[] newBytes = new byte[buf.remaining()];
        buf.get(newBytes);
        buf.position(savedPos);
        return newBytes;
    }

    /**
     * Put a short value out to the specified byte array position.
     * 
     * @param bytes
     *            the byte array
     * @param offset
     *            position in the array
     * @param val
     *            short to write out
     * @return incremented offset
     * @throws IllegalArgumentException
     *             if the byte array given doesn't have
     *             enough room at the offset specified.
     */
    public static int putShort(byte[] bytes, int offset, short val) {
        if (bytes.length - offset < SIZEOF_SHORT) {
            throw new IllegalArgumentException("Not enough room to put a short at"
                    + " offset " + offset + " in a " + bytes.length + " byte array");
        }
        bytes[offset + 1] = (byte) val;
        val >>= 8;
        bytes[offset] = (byte) val;
        return offset + SIZEOF_SHORT;
    }

    /**
     * Put a char value out to the specified byte array position.
     * 
     * @param bytes
     *            the byte array
     * @param offset
     *            position in the array
     * @param val
     *            short to write out
     * @return incremented offset
     * @throws IllegalArgumentException
     *             if the byte array given doesn't have
     *             enough room at the offset specified.
     */
    public static int putChar(byte[] bytes, int offset, char val) {
        if (bytes.length - offset < SIZEOF_CHAR) {
            throw new IllegalArgumentException("Not enough room to put a char at"
                    + " offset " + offset + " in a " + bytes.length + " byte array");
        }
        bytes[offset + 1] = (byte) val;
        val >>= 8;
        bytes[offset] = (byte) val;
        return offset + SIZEOF_CHAR;
    }

    /**
     * @param left
     *            left operand
     * @param right
     *            right operand
     * @return 0 if equal, < 0 if left is less than right, etc.
     */
    public static int compareTo(final byte[] left, final byte[] right) {
        return LexicographicalComparerHolder.BEST_COMPARER.
                compareTo(left, 0, left.length, right, 0, right.length);
    }

    /**
     * Lexicographically compare two arrays.
     * 
     * @param buffer1
     *            left operand
     * @param buffer2
     *            right operand
     * @param offset1
     *            Where to start comparing in the left buffer
     * @param offset2
     *            Where to start comparing in the right buffer
     * @param length1
     *            How much to compare from the left buffer
     * @param length2
     *            How much to compare from the right buffer
     * @return 0 if equal, < 0 if left is less than right, etc.
     */
    public static int compareTo(byte[] buffer1, int offset1, int length1,
            byte[] buffer2, int offset2, int length2) {
        return LexicographicalComparerHolder.BEST_COMPARER.
                compareTo(buffer1, offset1, length1, buffer2, offset2, length2);
    }

    interface Comparer<T> {
        int compareTo(T buffer1, int offset1, int length1, T buffer2, int offset2, int length2);
    }

    static Comparer<byte[]> lexicographicalComparerJavaImpl() {
        return LexicographicalComparerHolder.PureJavaComparer.INSTANCE;
    }

    /**
     * Provides a lexicographical comparer implementation; either a Java
     * implementation or a faster implementation based on {@link Unsafe}.
     * 
     * <p>
     * Uses reflection to gracefully fall back to the Java implementation if {@code Unsafe} isn't available.
     */
    static class LexicographicalComparerHolder {
        static final String UNSAFE_COMPARER_NAME =
                LexicographicalComparerHolder.class.getName() + "$UnsafeComparer";

        static final Comparer<byte[]> BEST_COMPARER = getBestComparer();

        /**
         * Returns the Unsafe-using Comparer, or falls back to the pure-Java
         * implementation if unable to do so.
         */
        static Comparer<byte[]> getBestComparer() {
            try {
                Class<?> theClass = Class.forName(UNSAFE_COMPARER_NAME);

                // yes, UnsafeComparer does implement Comparer<byte[]>
                @SuppressWarnings("unchecked")
                Comparer<byte[]> comparer =
                        (Comparer<byte[]>) theClass.getEnumConstants()[0];
                return comparer;
            } catch (Throwable t) { // ensure we really catch *everything*
                return lexicographicalComparerJavaImpl();
            }
        }

        enum PureJavaComparer implements Comparer<byte[]> {
            INSTANCE;

            @Override
            public int compareTo(byte[] buffer1, int offset1, int length1,
                    byte[] buffer2, int offset2, int length2) {
                // Short circuit equal case
                if (buffer1 == buffer2 &&
                        offset1 == offset2 &&
                        length1 == length2) {
                    return 0;
                }
                // Bring WritableComparator code local
                int end1 = offset1 + length1;
                int end2 = offset2 + length2;
                for (int i = offset1, j = offset2; i < end1 && j < end2; i++, j++) {
                    int a = (buffer1[i] & 0xff);
                    int b = (buffer2[j] & 0xff);
                    if (a != b) {
                        return a - b;
                    }
                }
                return length1 - length2;
            }
        }

        enum UnsafeComparer implements Comparer<byte[]> {
            INSTANCE;

            static final Unsafe theUnsafe;

            /** The offset to the first element in a byte array. */
            static final int BYTE_ARRAY_BASE_OFFSET;

            static {
                theUnsafe = (Unsafe) AccessController.doPrivileged(
                        new PrivilegedAction<Object>() {
                            @Override
                            public Object run() {
                                try {
                                    Field f = Unsafe.class.getDeclaredField("theUnsafe");
                                    f.setAccessible(true);
                                    return f.get(null);
                                } catch (NoSuchFieldException e) {
                                    // It doesn't matter what we throw;
                                    // it's swallowed in getBestComparer().
                                    throw new Error();
                                } catch (IllegalAccessException e) {
                                    throw new Error();
                                }
                            }
                        });

                BYTE_ARRAY_BASE_OFFSET = theUnsafe.arrayBaseOffset(byte[].class);

                // sanity check - this should never fail
                if (theUnsafe.arrayIndexScale(byte[].class) != 1) {
                    throw new AssertionError();
                }
            }

            static final boolean littleEndian =
                    ByteOrder.nativeOrder().equals(ByteOrder.LITTLE_ENDIAN);

            /**
             * Returns true if x1 is less than x2, when both values are treated as
             * unsigned.
             */
            static boolean lessThanUnsigned(long x1, long x2) {
                return (x1 + Long.MIN_VALUE) < (x2 + Long.MIN_VALUE);
            }

            /**
             * Lexicographically compare two arrays.
             * 
             * @param buffer1
             *            left operand
             * @param buffer2
             *            right operand
             * @param offset1
             *            Where to start comparing in the left buffer
             * @param offset2
             *            Where to start comparing in the right buffer
             * @param length1
             *            How much to compare from the left buffer
             * @param length2
             *            How much to compare from the right buffer
             * @return 0 if equal, < 0 if left is less than right, etc.
             */
            @Override
            public int compareTo(byte[] buffer1, int offset1, int length1,
                    byte[] buffer2, int offset2, int length2) {
                // Short circuit equal case
                if (buffer1 == buffer2 &&
                        offset1 == offset2 &&
                        length1 == length2) {
                    return 0;
                }
                int minLength = Math.min(length1, length2);
                int minWords = minLength / SIZEOF_LONG;
                int offset1Adj = offset1 + BYTE_ARRAY_BASE_OFFSET;
                int offset2Adj = offset2 + BYTE_ARRAY_BASE_OFFSET;

                /*
                 * Compare 8 bytes at a time. Benchmarking shows comparing 8 bytes at a
                 * time is no slower than comparing 4 bytes at a time even on 32-bit.
                 * On the other hand, it is substantially faster on 64-bit.
                 */
                for (int i = 0; i < minWords * SIZEOF_LONG; i += SIZEOF_LONG) {
                    long lw = theUnsafe.getLong(buffer1, offset1Adj + (long) i);
                    long rw = theUnsafe.getLong(buffer2, offset2Adj + (long) i);
                    long diff = lw ^ rw;

                    if (diff != 0) {
                        if (!littleEndian) {
                            return lessThanUnsigned(lw, rw) ? -1 : 1;
                        }

                        // Use binary search
                        int n = 0;
                        int y;
                        int x = (int) diff;
                        if (x == 0) {
                            x = (int) (diff >>> 32);
                            n = 32;
                        }

                        y = x << 16;
                        if (y == 0) {
                            n += 16;
                        } else {
                            x = y;
                        }

                        y = x << 8;
                        if (y == 0) {
                            n += 8;
                        }
                        return (int) (((lw >>> n) & 0xFFL) - ((rw >>> n) & 0xFFL));
                    }
                }

                // The epilogue to cover the last (minLength % 8) elements.
                for (int i = minWords * SIZEOF_LONG; i < minLength; i++) {
                    int a = (buffer1[offset1 + i] & 0xff);
                    int b = (buffer2[offset2 + i] & 0xff);
                    if (a != b) {
                        return a - b;
                    }
                }
                return length1 - length2;
            }
        }
    }

    /**
     * @param left
     *            left operand
     * @param right
     *            right operand
     * @return True if equal
     */
    public static boolean equals(final byte[] left, final byte[] right) {
        // Could use Arrays.equals?
        // noinspection SimplifiableConditionalExpression
        if (left == right)
            return true;
        if (left == null || right == null)
            return false;
        if (left.length != right.length)
            return false;
        if (left.length == 0)
            return true;

        // Since we're often comparing adjacent sorted data,
        // it's usual to have equal arrays except for the very last byte
        // so check that first
        if (left[left.length - 1] != right[right.length - 1])
            return false;

        return compareTo(left, right) == 0;
    }

    /**
     * @param left
     *            left operand
     * @param right
     *            right operand
     * @return True if equal
     */
    public static boolean equals(final byte[] left, int leftOffset, int leftLen,
            final byte[] right, int rightOffset, int rightLen) {
        // short circuit case
        if (left == right &&  leftOffset == rightOffset && leftLen == rightLen) {
            return true;
        }
        // different lengths fast check
        if (leftLen != rightLen) {
            return false;
        }
        if (leftLen == 0) {
            return true;
        }

        // Since we're often comparing adjacent sorted data,
        // it's usual to have equal arrays except for the very last byte
        // so check that first
        if (left[leftOffset + leftLen - 1] != right[rightOffset + rightLen - 1]) {
            return false;
        }

        return LexicographicalComparerHolder.BEST_COMPARER.
                compareTo(left, leftOffset, leftLen, right, rightOffset, rightLen) == 0;
    }

    /**
     * Return true if the byte array on the right is a prefix of the byte
     * array on the left.
     */
    public static boolean startsWith(byte[] bytes, byte[] prefix) {
        return bytes != null && prefix != null &&
                bytes.length >= prefix.length &&
                LexicographicalComparerHolder.BEST_COMPARER.
                        compareTo(bytes, 0, prefix.length, prefix, 0, prefix.length) == 0;
    }

    /**
     * @param a
     *            lower half
     * @param b
     *            upper half
     * @return New array that has a in lower half and b in upper half.
     */
    public static byte[] add(final byte[] a, final byte[] b) {
        return add(a, b, new byte[0]);
    }

    /**
     * @param a
     *            first third
     * @param b
     *            second third
     * @param c
     *            third third
     * @return New array made from a, b and c
     */
    public static byte[] add(final byte[] a, final byte[] b, final byte[] c) {
        byte[] result = new byte[a.length + b.length + c.length];
        System.arraycopy(a, 0, result, 0, a.length);
        System.arraycopy(b, 0, result, a.length, b.length);
        System.arraycopy(c, 0, result, a.length + b.length, c.length);
        return result;
    }

    /**
     * @param a
     *            array
     * @param length
     *            amount of bytes to grab
     * @return First <code>length</code> bytes from <code>a</code>
     */
    public static byte[] head(final byte[] a, final int length) {
        if (a.length < length) {
            return null;
        }
        byte[] result = new byte[length];
        System.arraycopy(a, 0, result, 0, length);
        return result;
    }

    /**
     * @param a
     *            array
     * @param length
     *            amount of bytes to snarf
     * @return Last <code>length</code> bytes from <code>a</code>
     */
    public static byte[] tail(final byte[] a, final int length) {
        if (a.length < length) {
            return null;
        }
        byte[] result = new byte[length];
        System.arraycopy(a, a.length - length, result, 0, length);
        return result;
    }

    /**
     * @param a
     *            array
     * @param length
     *            new array size
     * @return Value in <code>a</code> plus <code>length</code> prepended 0 bytes
     */
    public static byte[] padHead(final byte[] a, final int length) {
        byte[] padding = new byte[length];
        for (int i = 0; i < length; i++) {
            padding[i] = 0;
        }
        return add(padding, a);
    }

    /**
     * @param a
     *            array
     * @param length
     *            new array size
     * @return Value in <code>a</code> plus <code>length</code> appended 0 bytes
     */
    public static byte[] padTail(final byte[] a, final int length) {
        byte[] padding = new byte[length];
        for (int i = 0; i < length; i++) {
            padding[i] = 0;
        }
        return add(a, padding);
    }

    /**
     * Split passed range. Expensive operation relatively. Uses BigInteger math.
     * Useful splitting ranges for MapReduce jobs.
     * 
     * @param a
     *            Beginning of range
     * @param b
     *            End of range
     * @param num
     *            Number of times to split range. Pass 1 if you want to split
     *            the range in two; i.e. one split.
     * @return Array of dividing values
     */
    public static byte[][] split(final byte[] a, final byte[] b, final int num) {
        return split(a, b, false, num);
    }

    /**
     * Split passed range. Expensive operation relatively. Uses BigInteger math.
     * Useful splitting ranges for MapReduce jobs.
     * 
     * @param a
     *            Beginning of range
     * @param b
     *            End of range
     * @param inclusive
     *            Whether the end of range is prefix-inclusive or is considered an exclusive boundary. Automatic splits
     *            are generally exclusive and manual splits with an explicit range utilize an inclusive end of range.
     * @param num
     *            Number of times to split range. Pass 1 if you want to split the range in two; i.e. one split.
     * @return Array of dividing values
     */
    public static byte[][] split(final byte[] a, final byte[] b,
            boolean inclusive, final int num) {
        byte[][] ret = new byte[num + 2][];
        int i = 0;
        Iterable<byte[]> iter = iterateOnSplits(a, b, inclusive, num);
        if (iter == null)
            return null;
        for (byte[] elem : iter) {
            ret[i++] = elem;
        }
        return ret;
    }

    /**
     * Iterate over keys within the passed range, splitting at an [a,b) boundary.
     */
    public static Iterable<byte[]> iterateOnSplits(final byte[] a,
            final byte[] b, final int num)
    {
        return iterateOnSplits(a, b, false, num);
    }

    /**
     * Iterate over keys within the passed range.
     */
    public static Iterable<byte[]> iterateOnSplits(
            final byte[] a, final byte[] b, boolean inclusive, final int num)
    {
        byte[] aPadded;
        byte[] bPadded;
        if (a.length < b.length) {
            aPadded = padTail(a, b.length - a.length);
            bPadded = b;
        } else if (b.length < a.length) {
            aPadded = a;
            bPadded = padTail(b, a.length - b.length);
        } else {
            aPadded = a;
            bPadded = b;
        }
        if (compareTo(aPadded, bPadded) >= 0) {
            throw new IllegalArgumentException("b <= a");
        }
        if (num <= 0) {
            throw new IllegalArgumentException("num cannot be < 0");
        }
        byte[] prependHeader = { 1, 0 };
        final BigInteger startBI = new BigInteger(add(prependHeader, aPadded));
        final BigInteger stopBI = new BigInteger(add(prependHeader, bPadded));
        BigInteger diffBI = stopBI.subtract(startBI);
        if (inclusive) {
            diffBI = diffBI.add(BigInteger.ONE);
        }
        final BigInteger splitsBI = BigInteger.valueOf(num + 1);
        if (diffBI.compareTo(splitsBI) < 0) {
            return null;
        }
        final BigInteger intervalBI;
        try {
            intervalBI = diffBI.divide(splitsBI);
        } catch (Exception e) {
            LOGGER.error("Exception caught during division", e);
            return null;
        }

        final Iterator<byte[]> iterator = new Iterator<byte[]>() {
            private int i = -1;

            @Override
            public boolean hasNext() {
                return i < num + 1;
            }

            @Override
            public byte[] next() {
                i++;
                if (i == 0)
                    return a;
                if (i == num + 1)
                    return b;

                BigInteger curBI = startBI.add(intervalBI.multiply(BigInteger.valueOf(i)));
                byte[] padded = curBI.toByteArray();
                if (padded[1] == 0)
                    padded = tail(padded, padded.length - 2);
                else
                    padded = tail(padded, padded.length - 1);
                return padded;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

        };

        return new Iterable<byte[]>() {
            @Override
            public Iterator<byte[]> iterator() {
                return iterator;
            }
        };
    }

    /**
     * @param bytes
     *            array to hash
     * @param offset
     *            offset to start from
     * @param length
     *            length to hash
     * */
    public static int hashCode(byte[] bytes, int offset, int length) {
        int hash = 1;
        for (int i = offset; i < offset + length; i++)
            hash = (31 * hash) + (int) bytes[i];
        return hash;
    }
}