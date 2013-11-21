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
package com.unister.semweb.drums.bucket;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.unister.semweb.drums.TestUtils;
import com.unister.semweb.drums.api.DRUMS;
import com.unister.semweb.drums.api.DRUMSInstantiator;
import com.unister.semweb.drums.api.DRUMS.AccessMode;
import com.unister.semweb.drums.bucket.BucketSplitter;
import com.unister.semweb.drums.bucket.hashfunction.RangeHashFunction;
import com.unister.semweb.drums.storable.DummyKVStorable;
import com.unister.semweb.drums.utils.KeyUtils;
import com.unister.semweb.drums.utils.RangeHashFunctionTestUtils;

/**
 * Tests the {@link BucketSplitter} with non linear keys meaning that the keys within the buckets are not contiguous.
 * 
 * @author Nils Thieme
 */
public class BucketSplitterTestNonLinear {
    private static final String hashFunctionFilename = "/tmp/hash.hs";

    @Before
    public void initialise() throws IOException {
        FileUtils.deleteQuietly(new File(TestUtils.gp.databaseDirectory));
        new File(TestUtils.gp.databaseDirectory).mkdirs();
    }

    /**
     * Generates 10 test element with a width of 10 (keys are 1, 11, 21, 31, 41, 51, 61, 71, 81 and 91). The elements
     * are stored with the DRUMS. The bucket is split into two halves. So there will be two buckets with the following
     * elements:
     * <ul>
     * <li>First bucket: 1, 11, 21, 31, 41</li>
     * <li>Second bucket: 51, 61, 71, 81, 91</li>
     * </ul>
     * 
     * @throws Exception
     */
    @Test
    public void oneBucket2Split10Elements10Width() throws Exception {
        RangeHashFunction hashFunction = RangeHashFunctionTestUtils.createTestFunction(1, 10000, hashFunctionFilename,
                TestUtils.gp.keySize);
        DummyKVStorable[] testData = createAndFillDRUMS(10, 10, hashFunction);

        BucketSplitter<DummyKVStorable> splitter =
                new BucketSplitter<DummyKVStorable>(hashFunction, TestUtils.gp);
        splitter.splitAndStoreConfiguration(0, 2);

        DRUMS<DummyKVStorable> drumsAfterSplitting =
                DRUMSInstantiator.openTable(hashFunction, AccessMode.READ_ONLY, TestUtils.gp);
        // We must set the hash function because the hash function is loaded from the curious configuration file.
        drumsAfterSplitting.setHashFunction(hashFunction);

        List<DummyKVStorable> firstBucketElements = drumsAfterSplitting.read(0, 0, 100);
        List<DummyKVStorable> secondBucketElements = drumsAfterSplitting.read(1, 0, 100);

        Assert.assertEquals(5, firstBucketElements.size());
        Assert.assertEquals(5, secondBucketElements.size());

        for (int i = 0; i < firstBucketElements.size(); i++) {
            Assert.assertEquals(testData[i], firstBucketElements.get(i));
        }

        for (int i = 0; i < secondBucketElements.size(); i++) {
            Assert.assertEquals(testData[i + 5], secondBucketElements.get(i));
        }

        byte[][] expectedRanges = new byte[][] { KeyUtils.transformFromLong(41, TestUtils.gp.keySize),
                KeyUtils.transformFromLong(10000, TestUtils.gp.keySize) };
        Assert.assertTrue(examineHashFunction(hashFunction, expectedRanges));
    }

    /**
     * Creates 10 test data elements with a width of 100. Then the test data are written to the DRUMS and the bucket is
     * split into two halves.
     * 
     * @throws Exception
     */
    @Test
    public void oneBucket2Split10Elements100Width() throws Exception {
        int numberOfElements = 10;
        RangeHashFunction hashFunction = RangeHashFunctionTestUtils.createTestFunction(1, 10000, hashFunctionFilename,
                TestUtils.gp.keySize);
        DummyKVStorable[] testData = createAndFillDRUMS(numberOfElements, 100, hashFunction);

        BucketSplitter<DummyKVStorable> splitter = new BucketSplitter<DummyKVStorable>(hashFunction, TestUtils.gp);
        splitter.splitAndStoreConfiguration(0, 2);

        DRUMS<DummyKVStorable> drumsAfterSplitting = DRUMSInstantiator.openTable(hashFunction, AccessMode.READ_ONLY,
                TestUtils.gp);
        // We must set the hash function because the hash function is loaded from the curious configuration file.
        drumsAfterSplitting.setHashFunction(hashFunction);

        List<DummyKVStorable> firstBucketElements = drumsAfterSplitting.read(0, 0, 100);
        List<DummyKVStorable> secondBucketElements = drumsAfterSplitting.read(1, 0, 100);

        Assert.assertEquals(5, firstBucketElements.size());
        Assert.assertEquals(5, secondBucketElements.size());

        for (int i = 0; i < firstBucketElements.size(); i++) {
            Assert.assertEquals(testData[i], firstBucketElements.get(i));
        }

        for (int i = 0; i < secondBucketElements.size(); i++) {
            Assert.assertEquals(testData[i + 5], secondBucketElements.get(i));
        }

        byte[][] expectedRanges = new byte[][] { KeyUtils.transformFromLong(401, TestUtils.gp.keySize),
                KeyUtils.transformFromLong(10000, TestUtils.gp.keySize) };
        Assert.assertTrue(examineHashFunction(hashFunction, expectedRanges));
    }

    /**
     * Creates 5000 test data elements with a width of 100. Then the test data are written to the DRUMS and the bucket
     * is split into two halves.
     * 
     * @throws Exception
     */
    @Test
    public void oneBucket2Split5000Elements100Width() throws Exception {
        int numberOfElements = 5000;
        RangeHashFunction hashFunction = RangeHashFunctionTestUtils.createTestFunction(1, 500000,
                hashFunctionFilename, TestUtils.gp.keySize);
        DummyKVStorable[] testData = createAndFillDRUMS(numberOfElements, 100, hashFunction);

        BucketSplitter<DummyKVStorable> splitter = new BucketSplitter<DummyKVStorable>(hashFunction,
                TestUtils.gp);
        splitter.splitAndStoreConfiguration(0, 2);

        DRUMS<DummyKVStorable> drumsAfterSplitting = DRUMSInstantiator.openTable(hashFunction,
                AccessMode.READ_ONLY, TestUtils.gp);
        // We must set the hash function because the hash function is loaded from the curious configuration file.
        drumsAfterSplitting.setHashFunction(hashFunction);

        List<DummyKVStorable> firstBucketElements = drumsAfterSplitting.read(0, 0, 500000);
        List<DummyKVStorable> secondBucketElements = drumsAfterSplitting.read(1, 0, 500000);

        Assert.assertEquals(2500, firstBucketElements.size());
        Assert.assertEquals(2500, secondBucketElements.size());

        for (int i = 0; i < firstBucketElements.size(); i++) {
            Assert.assertEquals(testData[i], firstBucketElements.get(i));
        }

        for (int i = 0; i < secondBucketElements.size(); i++) {
            Assert.assertEquals(testData[i + 2500], secondBucketElements.get(i));
        }

        byte[][] expectedRanges = new byte[][] { KeyUtils.transformFromLong(249901, TestUtils.gp.keySize),
                KeyUtils.transformFromLong(500000, TestUtils.gp.keySize) };
        Assert.assertTrue(examineHashFunction(hashFunction, expectedRanges));
    }

    /**
     * Creates 5000 test data elements with a width of 100. Then the test data are written to the DRUMS and the bucket
     * is split into four halves.
     * 
     * @throws Exception
     */
    @Test
    public void oneBucket4Split5000Elements100Width() throws Exception {
        int numberOfElements = 5000;
        RangeHashFunction hashFunction = RangeHashFunctionTestUtils.createTestFunction(1, 500000,
                hashFunctionFilename, TestUtils.gp.keySize);
        DummyKVStorable[] testData = createAndFillDRUMS(numberOfElements, 100, hashFunction);

        BucketSplitter<DummyKVStorable> splitter = new BucketSplitter<DummyKVStorable>(hashFunction,
                TestUtils.gp);
        splitter.splitAndStoreConfiguration(0, 4);

        DRUMS<DummyKVStorable> drumsAfterSplitting = DRUMSInstantiator.openTable(hashFunction,
                AccessMode.READ_ONLY, TestUtils.gp);
        // We must set the hash function because the hash function is loaded from the curious configuration file.
        drumsAfterSplitting.setHashFunction(hashFunction);

        List<DummyKVStorable> firstBucketElements = drumsAfterSplitting.read(0, 0, 500000);
        List<DummyKVStorable> secondBucketElements = drumsAfterSplitting.read(1, 0, 500000);
        List<DummyKVStorable> thirdBucketElements = drumsAfterSplitting.read(2, 0, 500000);
        List<DummyKVStorable> fourthBucketElements = drumsAfterSplitting.read(3, 0, 500000);

        Assert.assertEquals(1250, firstBucketElements.size());
        Assert.assertEquals(1250, secondBucketElements.size());
        Assert.assertEquals(1250, thirdBucketElements.size());
        Assert.assertEquals(1250, fourthBucketElements.size());

        for (int i = 0; i < firstBucketElements.size(); i++) {
            Assert.assertEquals(testData[i], firstBucketElements.get(i));
        }

        for (int i = 0; i < secondBucketElements.size(); i++) {
            Assert.assertEquals(testData[i + 1250], secondBucketElements.get(i));
        }

        for (int i = 0; i < thirdBucketElements.size(); i++) {
            Assert.assertEquals(testData[i + 2500], thirdBucketElements.get(i));
        }

        for (int i = 0; i < fourthBucketElements.size(); i++) {
            Assert.assertEquals(testData[i + 3750], fourthBucketElements.get(i));
        }

        byte[][] expectedRanges = new byte[][] { KeyUtils.transformFromLong(124901, TestUtils.gp.keySize),
                KeyUtils.transformFromLong(249901, TestUtils.gp.keySize),
                KeyUtils.transformFromLong(374901, TestUtils.gp.keySize),
                KeyUtils.transformFromLong(500000, TestUtils.gp.keySize) };
        Assert.assertTrue(examineHashFunction(hashFunction, expectedRanges));
    }

    /**
     * Generates the specified number of test data with the specified width and stores them into an DRUMS. The specified
     * <code>hashFunction</code> is used for that purpose. The test data that were stored within the DRUMS are returned.
     * 
     * @param numberOfData
     * @param hashFunction
     * @param width
     * @return
     * @throws Exception
     */
    private DummyKVStorable[] createAndFillDRUMS(int numberOfData, int width, RangeHashFunction hashFunction)
            throws Exception {
        DummyKVStorable[] testData = TestUtils.generateTestdata(numberOfData, width);
        DRUMS<DummyKVStorable> drums = DRUMSInstantiator.createOrOpenTable(hashFunction, TestUtils.gp);
        drums.insertOrMerge(testData);
        drums.close();
        return testData;
    }

    /**
     * Takes the {@link RangeHashFunction} and an array of keys. The method compares the ranges of the hash function
     * with the expected keys. If they are the same <code>true</code> will be returned, otherwise <code>false</code>.
     * 
     * @param hashFunction
     * @param expectedBorders
     * @return
     */
    private boolean examineHashFunction(RangeHashFunction hashFunction, byte[][] expectedBorders) {
        byte[][] originalRanges = hashFunction.getRanges();
        if (originalRanges.length != expectedBorders.length) {
            return false;
        }

        for (int i = 0; i < originalRanges.length; i++) {
            int compareValue = KeyUtils.compareKey(originalRanges[i], expectedBorders[i]);
            if (compareValue != 0) {
                return false;
            }
        }
        return true;
    }

}
