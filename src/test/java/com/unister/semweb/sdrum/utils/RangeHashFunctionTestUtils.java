package com.unister.semweb.sdrum.utils;

import java.io.File;

import com.unister.semweb.sdrum.bucket.hashfunction.RangeHashFunction;

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
}
