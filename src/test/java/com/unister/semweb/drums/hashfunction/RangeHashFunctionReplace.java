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
package com.unister.semweb.drums.hashfunction;

import org.junit.Assert;
import org.junit.Test;

import com.unister.semweb.drums.bucket.hashfunction.RangeHashFunction;
import com.unister.semweb.drums.utils.Bytes;
import com.unister.semweb.drums.utils.KeyUtils;
import com.unister.semweb.drums.utils.RangeHashFunctionTestUtils;

/**
 * Tests the <code>replace</code> method of the {@link RangeHashFunction}.
 * 
 * @author Nils Thieme
 * 
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

        byte[] functionLine = testFunction.getUpperBound(0);
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

        byte[] newLine1 = Bytes.toBytes(0l);
        byte[] newLine2 = Bytes.toBytes(100l);
        byte[] newLine3 = Bytes.toBytes(1000l);
        byte[] newLine4 = Bytes.toBytes(10000l);

        byte[][] newLines = new byte[][] { newLine1, newLine2, newLine3, newLine4 };

        testFunction.replace(0, newLines);

        byte[][] functionLines = new byte[4][];
        String[] functionFilenames = new String[4];
        for (int i = 0; i < 4; i++) {
            functionLines[i] = testFunction.getUpperBound(i);
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
                rangeHashFunctionFilename, KEY_SIZE);

        byte[] newLine1 = Bytes.toBytes(1000l);
        byte[] newLine2 = Bytes.toBytes(2000l);
        byte[] newLine3 = Bytes.toBytes(3000l);
        byte[] newLine4 = Bytes.toBytes(4000l);

        byte[][] newLines = new byte[][] { newLine1, newLine2, newLine3, newLine4 };

        testFunction.replace(9, newLines);

        for (int i = 0; i < 4; i++) {
            Assert.assertEquals(0, KeyUtils.compareKey(newLines[i], testFunction.getUpperBound(9 + i)));
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

        byte[] newLine1 = Bytes.toBytes(300l);
        byte[] newLine2 = Bytes.toBytes(325l);
        byte[] newLine3 = Bytes.toBytes(350l);
        byte[] newLine4 = Bytes.toBytes(375l);

        byte[][] newLines = new byte[][] { newLine1, newLine2, newLine3, newLine4 };

        testFunction.replace(3, newLines);

        for (int i = 0; i < 4; i++) {
            Assert.assertEquals(0, KeyUtils.compareKey(newLines[i], testFunction.getUpperBound(3 + i)));
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

        byte[] newLine1 = Bytes.toBytes(300l);
        byte[][] newLines = new byte[][] { newLine1 };

        testFunction.replace(100, newLines);
    }
}