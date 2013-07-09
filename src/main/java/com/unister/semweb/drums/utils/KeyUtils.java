package com.unister.semweb.drums.utils;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Some very important and useful functions for the keys (byte-arrays) in DRUMS.
 * 
 * @author m.gleditzsch
 */
public class KeyUtils {
    private static final Logger log = LoggerFactory.getLogger(KeyUtils.class);

    /** transforms the given array of longs to an array of bytearrays */
    public static byte[][] transformToByteArray(long[] l) {
        byte[][] b = new byte[l.length][8];
        for (int i = 0; i < b.length; i++) {
            // if(l[i] <= 0) {
            // Logger.getLogger(TestUtils.class).warn("DRUMS only handles keys > 0.");
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
        return ByteBuffer.wrap(bytes).getLong();
    }

    /** Transforms the given long value into a byte array. */
    public static byte[] transformFromLong(long toTransform, int keySize) {
        ByteBuffer converter = ByteBuffer.allocate(8).putLong(toTransform);

        byte[] result = null;
        if (keySize >= 8) {
            byte[] leadingBytes = new byte[keySize - 8];
            result = ArrayUtils.addAll(leadingBytes, converter.array());
        } else {
            result = Arrays.copyOfRange(converter.array(), converter.array().length - keySize, 8);
        }
        return result;
    }

    /** Converts a long to a byte-array */
    public static byte[] convert(long key) {
        return transformFromLong(key, 8);
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
        return isNull(key, key.length);
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
     * We have two byte arrays and we want to know whether <code>compare</code> starts with <code>toBegin</code>. If so
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

    /** Generates a hashfunction */
    public static String generateHashFunction(byte[] min, byte[] max, int buckets, String suffix,
            String prefix) throws Exception {
        String[] Sbuckets = new String[buckets];
        for (int i = 0; i < buckets; i++) {
            Sbuckets[i] = i + "";
        }
        return generateHashFunction(min, max, Sbuckets, suffix, prefix);
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
        int[] key2 = toUnsignedInt(key);
        for (int i = 0; i < key.length; i++) {
            result.append(key2[i]);
            result.append(' ');
        }
        return result.toString();
    }

    /**
     * This method generates equally sized ranges between the given min and the given max. It return the max-value for
     * each range
     * 
     * @throws Exception
     */
    public static byte[][] getRanges(byte[] min, byte[] max, int numberOfRanges) throws Exception {
        byte[][] ranges = new byte[numberOfRanges][];
        if (compareKey(min, max) > 0) {
            throw new Exception("The given min is not larger than the max. Buckets could not be determined");
        }

        byte[] diff = subtractUnsigned(max, min);
        int[] iDiff = toUnsignedInt(diff);

        int[] iRange = new int[max.length];
        byte[] range = new byte[iDiff.length];
        int tmprest = 0;
        for (int i = 0; i < iDiff.length; i++) {
            iRange[i] += iDiff[i] / numberOfRanges;
            tmprest = (iDiff[i] % numberOfRanges) * 255;
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
        for (int i = 0; i < max.length; i++) {
            sb.append("b\t");
        }

        sb.append("filename\n");

        byte[] val = min;
        for (int i = 0; i < numberOfRanges; ++i) {
            val = sumUnsigned(val, range);
            if (compareKey(val, max) > 0) {
                val = max;
            }
            ranges[i] = val;
        }
        return ranges;
    }

    /**
     * Generates a long based range HashFunction
     * 
     * @throws Exception
     */
    public static String generateHashFunction(byte[] min, byte[] max, String[] buckets, String suffix,
            String prefix) throws Exception {
        if (compareKey(min, max) > 0) {
            throw new Exception("The given min is not larger than the max. Buckets could not be determined");
        }
        byte[][] ranges = getRanges(min, max, buckets.length);

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < min.length; i++) {
            sb.append("b").append("\t");
        }
        sb.append("filename").append("\n");

        for (int i = 0; i < buckets.length; ++i) {
            byte[] val = ranges[i];
            for (int j = 0; j < val.length; j++) {
                int k = val[j] & 0xff;
                sb.append(k + "\t");
            }
            sb.append(prefix + buckets[i] + suffix + "\n");
        }
        return sb.toString();
    }

    /**
     * Generates a range hash function with the given minimal and maximum value. Also the names of the buckets are
     * given, and the suffix and prefix of the buckets name.
     */
    public static String generateHashFunctionString(byte[] min, byte[] max, String[] buckets, String suffix,
            String prefix) throws Exception {
        byte[][] ranges = generateHashRanges(min, max, buckets.length);
        StringBuilder sb = new StringBuilder();

        for (int bucketCounter = 0; bucketCounter < buckets.length; bucketCounter++) {
            byte[] oneRange = ranges[bucketCounter];
            for (int i = 0; i < oneRange.length; i++) {
                int k = oneRange[i] & 0xff;
                sb.append(k + "\t");
            }
            sb.append(prefix + buckets[bucketCounter] + suffix + "\n");
        }
        return sb.toString();
    }

    /**
     * Gets a minimum and maximum value. It generates borders for ranges. Suppose your minium is 0 and the maximum value
     * is 1024, also you need 4 buckets, then the borders will be:
     * <p/>
     * <ul>
     * <li>256</li>
     * <li>512</li>
     * <li>768</li>
     * <li>1024</li>
     * </ul>
     */
    public static byte[][] generateHashRanges(byte[] min, byte[] max, int numberOfBuckets) throws Exception {
        if (compareKey(min, max) > 0) {
            throw new Exception("The given min is not larger than the max. Buckets could not be determined");
        }

        byte[] diff = subtractUnsigned(max, min);
        int[] iDiff = toUnsignedInt(diff);

        int[] iRange = new int[max.length];
        byte[] range = new byte[iDiff.length];
        int tmprest = 0;
        for (int i = 0; i < iDiff.length; i++) {
            iRange[i] += iDiff[i] / numberOfBuckets;
            tmprest = (iDiff[i] % numberOfBuckets) * 255;
            if (i < iDiff.length - 1) {
                iDiff[i + 1] += tmprest;
            }
            range[i] = (byte) iRange[i];
        }
        if (tmprest > 0) {
            iRange[max.length - 1]++;
            range[max.length - 1] = (byte) iRange[max.length - 1];
        }

        byte[][] result = new byte[numberOfBuckets][min.length];
        byte[] val = min;
        for (int i = 0; i < numberOfBuckets; ++i) {
            val = sumUnsigned(val, range);
            if (compareKey(val, max) > 0) {
                val = max;
            }
            result[i] = val;
        }
        return result;
    }

    public static void generateHashFunctionBigInteger(long min, long max, int buckets, int bucketSize, String suffix,
            String prefix) {
        int numberOfBytes = 15;

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
        int numberOfBuckets = 16;
        byte[] minKey = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
        byte[] maxKey = new byte[] { (byte) 255, (byte) 255, (byte) 255, (byte) 255, (byte) 255, (byte) 255,
                (byte) 255, (byte) 255, (byte) 255, (byte) 255, (byte) 255, (byte) 255, (byte) 255, (byte) 255,
                (byte) 255 };
        // byte[] maxKey = new byte[] { -128, -128, -128, -128, -128, -128, -128, -128, -128, -128, -128, -128 };
        // byte[] maxKey = new byte[] { 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127 };
        String[] buckets = new String[numberOfBuckets];
        for (int i = 0; i < numberOfBuckets; i++) {
            buckets[i] = String.valueOf(i);
        }

        String hashFunction = KeyUtils.generateHashFunction(minKey, maxKey, buckets, ".db", "");
        System.out.println(hashFunction);
    }
}
