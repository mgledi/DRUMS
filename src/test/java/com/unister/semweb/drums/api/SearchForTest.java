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
package com.unister.semweb.drums.api;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.unister.semweb.drums.GlobalParameters;
import com.unister.semweb.drums.api.DRUMS;
import com.unister.semweb.drums.api.DRUMSInitialisation;
import com.unister.semweb.drums.bucket.hashfunction.RangeHashFunction;
import com.unister.semweb.drums.storable.TestStorable;
import com.unister.semweb.drums.utils.RangeHashFunctionTestUtils;

/**
 * Tests the <code>select</code> method of the {@link DRUMS} class.
 * 
 * @author Nils Thieme
 * 
 */
public class SearchForTest {
    private static final String databaseDirectory = "/tmp/db";
    private GlobalParameters<TestStorable> globalParameters;

    private String rangeHashFunctionFilename = "/tmp/rangeHashFunction.txt";

    /** Initialises each test case by deleting the formerly created data and to create the {@link RangeHashFunction}. */
    @Before
    public void initialise() throws Exception {
        FileUtils.deleteQuietly(new File(databaseDirectory));
        FileUtils.deleteQuietly(new File(rangeHashFunctionFilename));

        globalParameters = new GlobalParameters<TestStorable>(new TestStorable());
        globalParameters.databaseDirectory = databaseDirectory;

        RangeHashFunction hashFunction = RangeHashFunctionTestUtils.createTestFunction(16, Integer.MAX_VALUE / 16,
                rangeHashFunctionFilename, globalParameters.keySize);
        hashFunction.writeToFile();
    }

    /** Some data are stored within the {@link DRUMS} and the keys of these data are retrieved. */
    @Test
    public void searchForKnownKeys() throws Exception {
        RangeHashFunction hashFunction = new RangeHashFunction(new File(rangeHashFunctionFilename));
        DRUMS<TestStorable> drums = DRUMSInitialisation.createOrOpenTable(hashFunction, globalParameters);

        TestStorable[] testdata = generateTestdata(100, 1);
        drums.insertOrMerge(testdata);
        drums.close();

        byte[][] keysToSearchFor = extractKeys(testdata);
        DRUMS<TestStorable> searchDrums = DRUMSInitialisation.createOrOpenTable(hashFunction, globalParameters);
        List<TestStorable> readData = searchDrums.select(keysToSearchFor);
        Assert.assertEquals(testdata.length, readData.size());
        Assert.assertTrue(equals(readData, testdata));
    }

    /**
     * Some data are written to the {@link DRUMS} but only unknwon keys keys are searced for. Nothin must be returned by
     * the <code>select</code> method.
     */
    @Test
    public void searchForUnknownKeys() throws Exception {
        RangeHashFunction hashFunction = new RangeHashFunction(new File(rangeHashFunctionFilename));
        DRUMS<TestStorable> drums = DRUMSInitialisation.createOrOpenTable(hashFunction, globalParameters);

        TestStorable[] testdata = generateTestdata(100, 1);
        drums.insertOrMerge(testdata);
        drums.close();

        byte[][] keysToSearchFor = generateKeys(200, 100);
        DRUMS<TestStorable> searchDrums = DRUMSInitialisation.createOrOpenTable(hashFunction, globalParameters);
        List<TestStorable> readData = searchDrums.select(keysToSearchFor);
        Assert.assertEquals(0, readData.size());
    }

    /** Some data are written to the {@link DRUMS}, known and unknown keys are searched for. */
    @Test
    public void searchForKnownAndUnknownKeys() throws Exception {
        RangeHashFunction hashFunction = new RangeHashFunction(new File(rangeHashFunctionFilename));
        DRUMS<TestStorable> drums = DRUMSInitialisation.createOrOpenTable(hashFunction, globalParameters);

        TestStorable[] testdata = generateTestdata(100, 1);
        drums.insertOrMerge(testdata);
        drums.close();

        byte[][] extractedKeys = extractKeys(testdata);
        byte[][] unknownKeys = generateKeys(200, 100);

        byte[][] keysToSearchFor = (byte[][]) ArrayUtils.addAll(extractedKeys, unknownKeys);
        DRUMS<TestStorable> searchDrums = DRUMSInitialisation.createOrOpenTable(hashFunction, globalParameters);
        List<TestStorable> readData = searchDrums.select(keysToSearchFor);
        Assert.assertEquals(testdata.length, readData.size());
        Assert.assertTrue(equals(readData, testdata));
    }

    /**
     * Creates test data that are only different in their key. The first key is given by an integer. Also the number of
     * test data are given.
     */
    private TestStorable[] generateTestdata(int numberOfTestdata, int beginKey) {
        TestStorable[] result = new TestStorable[numberOfTestdata];
        for (int i = 0; i < numberOfTestdata; i++) {
            byte[] key = convert(beginKey + i, globalParameters.keySize);
            TestStorable newStorable = new TestStorable();
            newStorable.key = key;
            result[i] = newStorable;
        }
        return result;
    }

    /** Generates the given number of keys beginning at the <code>startKey</code>. */
    private byte[][] generateKeys(int startKey, int numberOfKeys) {
        byte[][] result = new byte[numberOfKeys][];
        for (int i = 0; i < numberOfKeys; i++) {
            result[i] = convert(i + startKey, globalParameters.keySize);
        }
        return result;
    }

    /**
     * Converts the given <code>value</code> into an byte array. The final byte array will have
     * <code>overallBytes</code>.
     */
    private byte[] convert(int value, int overallBytes) {
        ByteBuffer buffer = ByteBuffer.allocate(overallBytes);
        buffer.putInt(overallBytes - 4, value);
        buffer.flip();
        return buffer.array();
    }

    /** Extracts the kys from the given test data. */
    private byte[][] extractKeys(TestStorable[] toExtractFrom) {
        byte[][] result = new byte[toExtractFrom.length][];
        for (int i = 0; i < toExtractFrom.length; i++) {
            result[i] = toExtractFrom[i].key;
        }
        return result;
    }

    /**
     * Examines whether the list and the array contains the same elements. Both must have the same size. Only the key
     * will be examined.
     */
    private boolean equals(List<TestStorable> list, TestStorable[] array) {
        if (list.size() != array.length) {
            return false;
        }

        for (TestStorable arrayEntry : array) {
            boolean isFound = false;
            for (TestStorable listEntry : list) {
                if (Arrays.equals(arrayEntry.key, listEntry.key)) {
                    isFound = true;
                    break;
                }
            }
            if (!isFound) {
                return false;
            }
        }
        return true;
    }

}
