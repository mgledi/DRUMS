package com.unister.semweb.sdrum.utils;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.unister.semweb.sdrum.GlobalParameters;
import com.unister.semweb.sdrum.bucket.hashfunction.RangeHashFunction;

public class KeyUtils {

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
    public static byte compareKey(byte[] key1, byte[] key2, int length) {
        int minLength = Math.min(key1.length, key2.length);
        for (int k = 0; k < minLength; k++) {
            if (key1[k] == key2[k]) {
                if (k == length - 1) {
                    return 0;
                }
                continue;
            }
            int k1 = key1[k] & 0xFF;
            int k2 = key2[k] & 0xFF;
            if (k1 < k2) {
                return -1;
            }
            if (k1 > k2) {
                return +1;
            }

        }
        if (key1.length < key2.length) {
            return -1;
        } else if (key1.length > key2.length) {
            return 1;
        }
        return 0;
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

    /** Generates a long based range HashFunction 
     * @throws Exception */
    public static String generateHashFunction(long min, long max, String[] buckets, int bucketSize, String suffix,
            String prefix) throws Exception {

        byte[] bmin = ByteBuffer.allocate(8).putLong(min).array();
        byte[] bmax = ByteBuffer.allocate(8).putLong(max).array();
        if(false) return generateHashFunction(bmin, bmax, buckets, bucketSize, suffix, prefix);
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
                    throw new Exception("The sum of the two given values is out of range.");
                }
                iSum[i - 1] += 1;
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
    public static byte[] subtractUnsigned(byte[] divisor, byte[] dividend) throws Exception {
        if (compareKey(divisor, dividend) <= 0) {
            throw new Exception("The given divisor is not larger than the dividend. This lead to an error");
        }
        byte[] diff = new byte[divisor.length];
        int[] imin = new int[dividend.length], imax = new int[divisor.length], iDiff = new int[divisor.length];
        for (int i = dividend.length - 1; i >= 0; i--) {
            imin[i] = dividend[i] & 0xFF;
            imax[i] = divisor[i] & 0xFF;
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
        if(tmprest > 0) {
            iRange[max.length - 1] ++;
            range[max.length - 1] = (byte)iRange[max.length - 1];
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
            sb.append(prefix + i + suffix + "\t" + bucketSize + "\n");
        }
        return sb.toString();
    }

    public static void main(String[] args) throws Exception {
        long l1 = -500;
        long l2 = -100;
        byte[] b1 =ByteBuffer.allocate(8).putLong(l1).array(); //{0, 0, 0, 100};
        byte[] b2 =ByteBuffer.allocate(8).putLong(l2).array(); //{0, 20, 20, 50};
        System.out.println(generateHashFunction(l1, l2, 10, 12, ".db", "/data/frontier/db"));
        System.out.println(generateHashFunction(b1, b2, 10, 12, ".db", "/data/frontier/db"));
    }
}
