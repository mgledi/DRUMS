/*
 * Copyright (C) 2012-2013 Unister GmbH
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */
package com.unister.semweb.drums.utils;

import java.io.File;
import java.math.BigInteger;
import java.util.Arrays;

import com.unister.semweb.drums.bucket.hashfunction.RangeHashFunction;
import com.unister.semweb.drums.utils.KeyUtils;

/**
 * Utility class for handling {@link RangeHashFunction} easier.
 * 
 * @author n.thieme
 */
public class RangeHashFunctionTestUtils {
    /**
     * Creates a {@link RangeHashFunction} with the specified number of ranges. Each range has the specified width. All
     * buckets have the same size as specified. The first range begins with the "rangeWidth".
     * 
     * @param numberOfRanges
     * @param rangeWidth
     * @return
     */
    public static RangeHashFunction createTestFunction(int numberOfRanges, int rangeWidth, String filename, int keySize) {
        byte[][] ranges = new byte[numberOfRanges][];
        String[] filenames = new String[numberOfRanges];
        for (int i = 0; i < numberOfRanges; i++) {
            byte[] oneLine = KeyUtils.transformFromLong((i + 1) * rangeWidth, keySize);
            ranges[i] = oneLine;
            filenames[i] = i + ".db";
        }

        RangeHashFunction result = new RangeHashFunction(ranges, filenames, filename);
        return result;
    }

    /** Generates a {@link RangeHashFunction} equally distributed over the number of the given ranges. */
    public static RangeHashFunction createTestFunction(int numberOfRanges, String filename, int keySize) {
        byte[] maxValue = new byte[keySize];
        Arrays.fill(maxValue, (byte) -1);

        BigInteger maxValueEasy = new BigInteger(1, maxValue);
        BigInteger rangeWidth = maxValueEasy.divide(BigInteger.valueOf(numberOfRanges));

        System.out.println(Arrays.toString(rangeWidth.toByteArray()));

        byte[][] ranges = new byte[numberOfRanges][];
        String[] filenames = new String[numberOfRanges];
        for (int i = 0; i < numberOfRanges; i++) {
            BigInteger currentRangeValue = rangeWidth.multiply(BigInteger.valueOf(i + 1));

            // We must remove leading 0s bytes.
            byte[] convertedValue = currentRangeValue.toByteArray();
            byte[] finalValue = Arrays.copyOfRange(convertedValue, convertedValue.length - keySize,
                    convertedValue.length);
            ranges[i] = finalValue;
            filenames[i] = i + ".db";
        }

        RangeHashFunction result = new RangeHashFunction(ranges, filenames, filename);
        return result;
    }
}
