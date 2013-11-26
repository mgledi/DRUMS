/* Copyright (C) 2012-2013 Unister GmbH
 * 
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA. */
package com.unister.semweb.drums.utils;

import java.util.Arrays;

import org.w3c.dom.ranges.RangeException;

import com.unister.semweb.drums.bucket.hashfunction.RangeHashFunction;

/**
 * Some very important and useful functions for the keys (byte-arrays) in DRUMS.
 * 
 * @author Martin Nettling
 */
public class KeyUtils {
    /** transforms the given array of longs to an array of byte-arrays */
    public static byte[][] toByteArray(long[] l) {
        byte[][] b = new byte[l.length][];
        for (int i = 0; i < b.length; i++) {
            b[i] = Bytes.toBytes(l[i]);
        }
        return b;
    }

    /**
     * Checks if the elements of the given key up the given length are 0, or the whole array is null.
     * 
     * @param key
     * @param length
     * @return true, if the key is null or all elements in the array are 0.
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
     * Checks if the elements of the given key are 0, or the whole array is null.
     * 
     * @param key
     * @param length
     * @return true, if the key is null or all elements in the array are 0.
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
     * @return <0 if key1 < key2<br>
     *         0 if key1 == key2<br>
     *         >0 if key1 > key2
     */
    public static int compareKey(byte[] key1, byte[] key2) {
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
     * @return <0 if key1 < key2<br>
     *         0 if key1 == key2<br>
     *         >0 if key1 > key2
     */
    // TODO If the given keys have different length the function will not give the right result.
    public static int compareKey(byte[] key1, byte[] key2, int length) {
        return Bytes.compareTo(key1, 0, length, key2, 0, length);
    }

    /**
     * Generates a {@link RangeHashFunction}.
     * 
     * @param min
     *            the minimal value to expect
     * @param max
     *            the maximal value to expect
     * @param buckets
     *            the number of buckets
     * @param suffix
     *            the suffix for all files
     * @param prefix
     *            a prefix for all files
     * @return String representation of the the RangeHashFunction
     * @throws Exception
     */
    public static String generateHashFunction(byte[] min, byte[] max, int buckets, String suffix, String prefix)
            throws Exception {
        String[] Sbuckets = new String[buckets];
        for (int i = 0; i < buckets; i++) {
            Sbuckets[i] = i + "";
        }
        return generateRangeHashFunction(min, max, Sbuckets, suffix, prefix);
    }

    /**
     * Generates a {@link RangeHashFunction}.
     * 
     * @param min
     *            the minimal value to expect
     * @param max
     *            the maximal value to expect
     * @param buckets
     *            an array with the names of the buckets
     * @param suffix
     *            the suffix for all files
     * @param prefix
     *            a prefix for all files
     * @return String representation of the the RangeHashFunction
     * @throws Exception
     */
    public static String generateRangeHashFunction(byte[] min, byte[] max, String[] buckets, String suffix,
            String prefix) throws Exception {
        if (compareKey(min, max) > 0) {
            throw new Exception("The given min is not larger than the max. Buckets could not be determined");
        }
        byte[][] ranges = getMaxValsPerRange(min, max, buckets.length);

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
     * 
     * @param min
     *            the minimal value to expect
     * @param max
     *            the maximal value to expect
     * @param num
     *            the number of ranges
     * @return an array containing the maximum value per range.
     */
    public static byte[][] getMaxValsPerRange(byte[] min, byte[] max, int num) {
        return Arrays.copyOfRange(Bytes.split(min, max, false, num - 1), 1, num + 1);
    }

    /**
     * Generates a string representation of the given key
     * 
     * @param key
     * @return the String representation of the given key
     */
    public static String toStringUnsignedInt(byte[] key) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < key.length; i++) {
            result.append(key[i] & 0xFF);
            result.append(' ');
        }
        return result.toString();
    }
}
