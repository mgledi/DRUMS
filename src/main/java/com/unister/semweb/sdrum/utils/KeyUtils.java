package com.unister.semweb.sdrum.utils;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KeyUtils {
    private static final Logger log = LoggerFactory.getLogger(KeyUtils.class);

    /** transforms the given array of longs to an array of bytearrays */
    public static byte[][] transformToByteArray(long[] l) {
        byte[][] b = new byte[l.length][8];
        for (int i = 0; i < b.length; i++) {
            // if(l[i] <= 0) {
            // Logger.getLogger(TestUtils.class).warn("SDRUM only handles keys > 0.");
            // }
            ByteBuffer.wrap(b[i]).putLong(l[i]);
        }
        return b;
    }

    /**
     * Transforms the given byte array (8 bytes) to the corresponding long value.
     * 
     * @param bytes
     * @return
     */
    public static long transformFromByte(byte[] bytes) {
        if (bytes.length != 8)
            return 0;
        ByteBuffer converter = ByteBuffer.wrap(bytes);
        return converter.getLong();
    }

    /** Transforms the given long value into a byte array. */
    public static byte[] transformFromLong(long toTransform, int keySize) {
        ByteBuffer converter = ByteBuffer.allocate(8);
        converter.putLong(toTransform);
        byte[] leadingBytes = new byte[keySize - 8];
        for (int i = 0; i < leadingBytes.length; i++) {
            leadingBytes[i] = 0;
        }

        byte[] result = ArrayUtils.addAll(leadingBytes, converter.array());
        return result;
    }

    /**
     * Cheks if the elements of the given key up the given length are 0, or the whole array is NULL
     * 
     * @param key
     * @param length
     * @return
     */
    public static boolean isNull(byte[] key, int length) {
        if (key == null) {
            return true;
        }
        for (int i = 0; i < Math.min(key.length, length); i++) {
            if (key[i] != 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * Cheks if the elements of the given key are 0, or the whole array is NULL
     * 
     * @param key
     * @param length
     * @return
     */
    public static boolean isNull(byte[] key) {
        if (key == null) {
            return true;
        }
        for (byte b : key) {
            if (b != 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * Compares the two byte-arrays on the basis of unsigned bytes. The array will be compared by each element up to the
     * length of the smaller array. If all elements are equal and the array are not equal sized, the larger array is
     * seen as larger.
     * 
     * @param key1
     * @param key2
     * 
     * @return -1 if key1 < key2<br>
     *         0 if key1 == key2<br>
     *         1 if key1 > key2
     */
    public static byte compareKey(byte[] key1, byte[] key2) {
        return compareKey(key1, key2, Math.min(key1.length, key2.length));
    }

    /**
     * Compares the two byte-arrays on the basis of unsigned bytes. The array will be compared by each element up to the
     * length of the smaller array or length. If all elements are equal and the array are not equal sized, the larger
     * array is seen as larger.<br>
     * The parameter length shows, up to which position the arrays are compared
     * 
     * @param key1
     * @param key2
     * @param length
     * 
     * @return -1 if key1 < key2<br>
     *         0 if key1 == key2<br>
     *         1 if key1 > key2
     */
    // TODO If the given keys have different length the function will not give the right result.
    public static byte compareKey(byte[] key1, byte[] key2, int length) {
        int minLength = Math.min(key1.length, key2.length);
        for (int k = 0; k < minLength; k++) {
            if (key1[k] == key2[k]) {
                if (k == length - 1) {
                    return 0;
                }
                continue;
            }

            // byte k1 = key1[k];
            // byte k2 = key2[k];
            int k1 = key1[k] & 0xFF;
            int k2 = key2[k] & 0xFF;
            if (k1 < k2) {
                return -1;
            }
            if (k1 > k2) {
                return +1;
            }

        }

        log.error("uncomparable values compared");
        if (key1.length < key2.length) {
            return -1;
        } else if (key1.length > key2.length) {
            return 1;
        }
        return 0;
    }

    /**
     * We have two byte arrays and we want to know whether <code>compare</code> starts <code>toBegin</code>. If so
     * <code>true</code> will be returned. If <code>toBegin</code> has more bytes then <code>compare</code> it will
     * return <code>false</code>.
     * 
     * @param toBegin
     * @param compare
     * @return
     */
    public static boolean startsWith(byte[] toBegin, byte[] compare) {
        if (toBegin.length > compare.length) {
            return false;
        }

        for (int i = 0; i < toBegin.length; i++) {
            if (toBegin[i] != compare[i]) {
                return false;
            }
        }
        return true;
    }

    public static String generateHashFunction(byte[] min, byte[] max, int buckets, int bucketSize, String suffix,
            String prefix) throws Exception {
        String[] Sbuckets = new String[buckets];
        for (int i = 0; i < buckets; i++) {
            Sbuckets[i] = i + "";
        }
        return generateHashFunction(min, max, Sbuckets, bucketSize, suffix, prefix);
    }

    public static String generateHashFunction(long min, long max, int buckets, int bucketSize, String suffix,
            String prefix) throws Exception {
        String[] Sbuckets = new String[buckets];
        for (int i = 0; i < buckets; i++) {
            Sbuckets[i] = i + "";
        }
        return generateHashFunction(min, max, Sbuckets, bucketSize, suffix, prefix);
    }

    /**
     * Generates a long based range HashFunction
     * 
     * @throws Exception
     */
    public static String generateHashFunction(long min, long max, String[] buckets, int bucketSize, String suffix,
            String prefix) throws Exception {
        // if(false) return generateHashFunction(bmin, bmax, buckets, bucketSize, suffix, prefix);
        long range = (long) Math.ceil((double) (max - min) / buckets.length);
        StringBuilder sb = new StringBuilder();

        long val = min;
        for (int i = 0; i < buckets.length; ++i) {
            val += range;
            if (val >= max) {
                val = max;
            }   
            byte[] bval = ByteBuffer.allocate(8).putLong(val).array();
            for (int j = 0; j < bval.length; j++) {
                int k = bval[j] & 0xff;
                sb.append(k + "\t");
            }
            sb.append(prefix + i + suffix + "\t" + bucketSize + "\n");
        }
        return sb.toString();
    }

    /**
     * this is a unsigned operation. The returned byte-array therefore should also be positive.
     * 
     * @throws Exception
     */
    public static byte[] sumUnsigned(byte[] b1, byte[] b2) throws Exception {
        byte[] sum = new byte[b1.length];
        int[] imin = new int[b2.length], imax = new int[b1.length], iSum = new int[b1.length];
        for (int i = b2.length - 1; i >= 0; i--) {
            imin[i] = b2[i] & 0xFF;
            imax[i] = b1[i] & 0xFF;
            iSum[i] = imax[i] + imin[i] + iSum[i];
            if (iSum[i] > 255) {
                iSum[i] -= 255;
                if (i == 0) {
                    System.err.println("The sum of the two given values is out of range.\n   " + Arrays.toString(b1)
                            + "\n + " + Arrays.toString(b2) + "\n = " + Arrays.toString(iSum));
                } else {

                    iSum[i - 1] += 1;
                }
            }
            sum[i] = (byte) iSum[i];
        }
        return sum;
    }

    /**
     * this is a unsigned operation. The returned byte-array therefore should also be positive.
     * 
     * @throws Exception
     */
    public static byte[] subtractUnsigned(byte[] minuend, byte[] subtrahend) throws Exception {
        if (compareKey(minuend, subtrahend) <= 0) {
            throw new Exception("The given divisor is not larger than the dividend. This lead to an error");
        }
        byte[] diff = new byte[minuend.length];
        int[] imin = new int[subtrahend.length], imax = new int[minuend.length], iDiff = new int[minuend.length];
        for (int i = subtrahend.length - 1; i >= 0; i--) {
            imin[i] = subtrahend[i] & 0xFF;
            imax[i] = minuend[i] & 0xFF;
            iDiff[i] = imax[i] - imin[i] + iDiff[i];
            if (iDiff[i] < 0) {
                iDiff[i] += 255;
                iDiff[i - 1] -= 1;

            }
            diff[i] = (byte) iDiff[i];
        }
        return diff;
    }

    public static int[] toUnsignedInt(byte[] signed) {
        int[] unsigned = new int[signed.length];
        for (int i = signed.length - 1; i >= 0; i--)
            unsigned[i] = signed[i] & 0xFF;
        return unsigned;
    }

    /** Makes a string representation from the key */
    public static String transform(byte[] key) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < key.length; i++) {
            result.append(key[i]);
            result.append(' ');
        }
        return result.toString();
    }

    /**
     * Generates a long based range HashFunction
     * 
     * @throws Exception
     */
    public static String generateHashFunction(byte[] min, byte[] max, String[] buckets, int bucketSize, String suffix,
            String prefix) throws Exception {
        if (compareKey(min, max) > 0) {
            throw new Exception("The given min is not larger than the max. Buckets could not be determined");
        }

        byte[] diff = subtractUnsigned(max, min);
        int[] iDiff = toUnsignedInt(diff);

        int[] iRange = new int[max.length];
        byte[] range = new byte[iDiff.length];
        int tmprest = 0;
        for (int i = 0; i < iDiff.length; i++) {
            iRange[i] += iDiff[i] / buckets.length;
            tmprest = (iDiff[i] % buckets.length) * 255;
            if (i < iDiff.length - 1) {
                iDiff[i + 1] += tmprest;
            }
            range[i] = (byte) iRange[i];
        }
        if (tmprest > 0) {
            iRange[max.length - 1]++;
            range[max.length - 1] = (byte) iRange[max.length - 1];
        }
        StringBuilder sb = new StringBuilder();

        byte[] val = min;
        for (int i = 0; i < buckets.length; ++i) {
            val = sumUnsigned(val, range);
            if (compareKey(val, max) > 0) {
                val = max;
            }
            for (int j = 0; j < val.length; j++) {
                int k = val[j] & 0xff;
                sb.append(k + "\t");
            }
            sb.append(prefix + buckets[i] + suffix + "\t" + bucketSize + "\n");
        }
        return sb.toString();
    }

    public static void generateHashFunctionBigInteger(long min, long max, int buckets, int bucketSize, String suffix,
            String prefix) {
        int numberOfBytes = 12;

        BigInteger bigMin = BigInteger.valueOf(min);
        BigInteger bigMax = BigInteger.valueOf(max);
        BigInteger bigBuckets = BigInteger.valueOf(buckets);

        BigInteger bigMaxMinDifference = bigMax.subtract(bigMin);
        BigInteger bigRange = bigMaxMinDifference.divide(bigBuckets);

        BigInteger bigVal = bigMin;

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < numberOfBytes; i++) {
            sb.append("b\t");
        }

        sb.append("filename\t");
        sb.append("bucketsize\n");
        for (int i = 0; i < buckets; ++i) {
            bigVal = bigVal.add(bigRange);
            if (bigVal.compareTo(bigMax) == 0 || bigVal.compareTo(bigMax) == 1) {
                bigVal = bigMax;
            }

            byte[] bval = ByteBuffer.allocate(12).put(bigVal.toByteArray()).array();

            for (int j = 0; j < bval.length; j++) {
                int k = bval[j] & 0xff;
                sb.append(k + "\t");
            }
            sb.append(prefix + i + suffix + "\t" + bucketSize + "\n");
        }

        // BigDecimal bigMin = new BigDecimal(min);
        // BigDecimal bigMax = new BigDecimal(max);
        // BigDecimal bigBuckets = new BigDecimal(buckets);
        //
        // BigDecimal bigMaxMinDifference = bigMax.subtract(bigMin);
        // BigDecimal bigRange = bigMaxMinDifference.divide(bigBuckets, RoundingMode.CEILING);
        //
        // BigDecimal bigVal = new BigDecimal(bigMin.toBigInteger());
        //
        // StringBuilder sb = new StringBuilder();
        // for (int i = 0; i < buckets; ++i) {
        // bigVal = bigVal.add(bigRange);
        // if (bigVal.compareTo(bigMax) == 0 || bigVal.compareTo(bigMax) == 1) {
        // bigVal = new BigDecimal(bigMax.toBigInteger());
        // }
        //
        // byte[] bval = ByteBuffer.allocate(8).putLong(bigVal.longValue()).array();
        //
        // for (int j = 0; j < bval.length; j++) {
        // int k = bval[j] & 0xff;
        // sb.append(k + "\t");
        // }
        // sb.append(prefix + i + suffix + "\t" + bucketSize + "\n");
        // }

        System.out.println(sb.toString());
    }

    public static void main(String[] args) throws Exception {
        // byte[] h0 = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
        // byte[] h1 = new byte[] { (byte) 64, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1 };
        // byte[] h2 = new byte[] { (byte) 128, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1 };
        // byte[] h3 = new byte[] { (byte) -64, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1 };
        // byte[] h4 = new byte[] { (byte) -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1 };
        //
        // String result = generateHashFunction(h3, h4, 4096, 10000, ".db", "");
        // System.out.println(result);

        generateHashFunctionBigInteger(Long.MIN_VALUE, Long.MAX_VALUE, 128, 10000, ".db", "");
    }
}
