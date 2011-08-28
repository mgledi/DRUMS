package com.unister.semweb.sdrum.utils;

import java.nio.ByteBuffer;

import org.apache.log4j.Logger;

import com.unister.semweb.sdrum.TestUtils;

public class KeyUtils {

    
    /** transforms the given array of longs to an array of bytearrays */
    public static byte[][] transformToByteArray(long[] l) {
        byte[][] b = new byte[l.length][8];
        for (int i = 0; i < b.length; i++) {
            if(l[i] <= 0) {
                Logger.getLogger(TestUtils.class).warn("SDRUM only handles keys > 0.");
            }
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
        for (int k = 0; k < Math.min(key1.length, key2.length); k++) {
            if (key1[k] == key2[k]) {
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
            if (k == length) {
                return 0;
            }
        }
        if (key1.length < key2.length) {
            return -1;
        } else if (key1.length > key2.length) {
            return 1;
        }
        return 0;
    }
}
