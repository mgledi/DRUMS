package com.unister.semweb.sdrum.hashfunction;

import java.io.File;

import org.junit.Assert;
import org.junit.Test;

import com.unister.semweb.sdrum.bucket.hashfunction.RangeHashFunction;
import com.unister.semweb.sdrum.utils.KeyUtils;
import com.unister.semweb.sdrum.utils.RangeHashFunctionTestUtils;

/**
 * Tests the <code>replace</code> method of the {@link RangeHashFunction}.
 * 
 * @author n.thieme
 */
public class RangeHashFunctionReplace {
    private static final String rangeHashFunctionFilename = "/tmp/rangeHashFunctionTest.txt";
    private static final int KEY_SIZE = 8;

    /**
     * Creates a {@link RangeHashFunction} with one line and replaces this lines with one single line.
     */
    @Test
    public void oneLineOneLine() {
        RangeHashFunction testFunction = RangeHashFunctionTestUtils.createTestFunction(1, 0,
                rangeHashFunctionFilename, KEY_SIZE);

        byte[] newLine = new byte[] { 0, 0, 0, 0, 0, 0, 0, (byte) 200 };
        byte[][] newLines = new byte[][] { newLine };

        testFunction.replace(0, newLines);

        byte[] functionLine = testFunction.getMaxRange(0);
        String functionFilename = testFunction.getFilename(0);

        Assert.assertEquals(0, KeyUtils.compareKey(newLine, functionLine));
        Assert.assertEquals("0_0.db", functionFilename);
    }

    /**
     * Creates a {@link RangeHashFunction} with one line and replaces this line with four lines.
     */
    @Test
    public void oneLineFourLines() {
        RangeHashFunction testFunction = RangeHashFunctionTestUtils.createTestFunction(1, 0,
                rangeHashFunctionFilename, KEY_SIZE);

        byte[] newLine1 = KeyUtils.transformFromLong(0, KEY_SIZE);
        byte[] newLine2 = KeyUtils.transformFromLong(100, KEY_SIZE);
        byte[] newLine3 = KeyUtils.transformFromLong(1000, KEY_SIZE);
        byte[] newLine4 = KeyUtils.transformFromLong(10000, KEY_SIZE);

        byte[][] newLines = new byte[][] { newLine1, newLine2, newLine3, newLine4 };

        testFunction.replace(0, newLines);

        byte[][] functionLines = new byte[4][];
        String[] functionFilenames = new String[4];
        for (int i = 0; i < 4; i++) {
            functionLines[i] = testFunction.getMaxRange(i);
            functionFilenames[i] = testFunction.getFilename(i);
        }

        for (int i = 0; i < 4; i++) {
            Assert.assertEquals(0, KeyUtils.compareKey(newLines[i], functionLines[i]));
            Assert.assertEquals("0_" + i + ".db", functionFilenames[i]);
        }
    }

    /**
     * Creates a {@link RangeHashFunction} with several lines. The last line will be replaced by four new lines.
     */
    @Test
    public void severalLinesLastFourLines() {
        RangeHashFunction testFunction = RangeHashFunctionTestUtils.createTestFunction(10, 100,
                rangeHashFunctionFilename, 8);

        byte[] newLine1 = KeyUtils.transformFromLong(1000, 8);
        byte[] newLine2 = KeyUtils.transformFromLong(2000, 8);
        byte[] newLine3 = KeyUtils.transformFromLong(3000, 8);
        byte[] newLine4 = KeyUtils.transformFromLong(4000, 8);

        byte[][] newLines = new byte[][] { newLine1, newLine2, newLine3, newLine4 };

        testFunction.replace(9, newLines);

        for (int i = 0; i < 4; i++) {
            Assert.assertEquals(0, KeyUtils.compareKey(newLines[i], testFunction.getMaxRange(9 + i)));
            Assert.assertEquals("9_" + i + ".db", testFunction.getFilename(9 + i));
        }
    }

    /**
     * Creates a {@link RangeHashFunction} with 10 entries. The third entry will be replaced four new lines.
     */
    @Test
    public void severalLinesMidFourLines() {
        RangeHashFunction testFunction = RangeHashFunctionTestUtils.createTestFunction(10, 100,
                rangeHashFunctionFilename, KEY_SIZE);

        byte[] newLine1 = KeyUtils.transformFromLong(300, KEY_SIZE);
        byte[] newLine2 = KeyUtils.transformFromLong(325, KEY_SIZE);
        byte[] newLine3 = KeyUtils.transformFromLong(350, KEY_SIZE);
        byte[] newLine4 = KeyUtils.transformFromLong(375, KEY_SIZE);

        byte[][] newLines = new byte[][] { newLine1, newLine2, newLine3, newLine4 };

        testFunction.replace(3, newLines);

        for (int i = 0; i < 4; i++) {
            Assert.assertEquals(0, KeyUtils.compareKey(newLines[i], testFunction.getMaxRange(3 + i)));
            Assert.assertEquals("3_" + i + ".db", testFunction.getFilename(3 + i));
        }
    }

    /**
     * Tries to replace one line within the {@link RangeHashFunction} that doesn't exist.
     */
    @Test(expected = IllegalArgumentException.class)
    public void invalidBucketId() {
        RangeHashFunction testFunction = RangeHashFunctionTestUtils.createTestFunction(10, 100,
                rangeHashFunctionFilename, KEY_SIZE);

        byte[] newLine1 = KeyUtils.transformFromLong(300, KEY_SIZE);
        byte[][] newLines = new byte[][] { newLine1 };

        testFunction.replace(100, newLines);
    }
}