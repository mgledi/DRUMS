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

        RangeHashFunction result = new RangeHashFunction(ranges, filenames, new File(filename));
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

        RangeHashFunction result = new RangeHashFunction(ranges, filenames, new File(filename));
        return result;
    }
}
