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
import org.junit.Ignore;
import org.junit.Test;

import com.unister.semweb.drums.TestUtils;
import com.unister.semweb.drums.api.DRUMS;
import com.unister.semweb.drums.api.DRUMSInstantiator;
import com.unister.semweb.drums.api.DRUMS.AccessMode;
import com.unister.semweb.drums.bucket.BucketSplitter;
import com.unister.semweb.drums.bucket.hashfunction.RangeHashFunction;
import com.unister.semweb.drums.storable.DummyKVStorable;
import com.unister.semweb.drums.utils.RangeHashFunctionTestUtils;

/**
 * This class is for testing the BucketSplitter The tests creates one database file and tries to split this file. Also
 * the test data are linear meaning that there is no gap between the keys within the data.
 * 
 * @author Martin Nettling, Nils Thieme
 */
public class BucketSplitterTestLinear {
    private static final String hashFunctionFilename = "/tmp/hash.hs";

    @Before
    public void initialise() throws IOException {
        FileUtils.deleteQuietly(new File(TestUtils.gp.databaseDirectory));
        new File(TestUtils.gp.databaseDirectory).mkdirs();
    }

    /**
     * Generates a bucket with 100 elements and splits this bucket into two ones.
     * 
     * @throws Exception
     */
    @Test
    public void oneBucket2Split() throws Exception {
        int numberOfElements = 100;
        RangeHashFunction hashFunction = RangeHashFunctionTestUtils.createTestFunction(1, 100,
                hashFunctionFilename, TestUtils.gp.keySize);
        DummyKVStorable[] testData = createAndFillDRUMS(numberOfElements, hashFunction);

        BucketSplitter<DummyKVStorable> splitter = new BucketSplitter<DummyKVStorable>(hashFunction,
                TestUtils.gp);
        
        splitter.splitAndStoreConfiguration(0, 2);

        DRUMS<DummyKVStorable> drumsAfterSplitting = DRUMSInstantiator.openTable(hashFunction,
                AccessMode.READ_ONLY, TestUtils.gp);
        // We must set the hash function because the hash function is loaded from the curious configuration file.
        drumsAfterSplitting.setHashFunction(hashFunction);

        List<DummyKVStorable> firstBucketElements = drumsAfterSplitting.read(0, 0, 50);
        List<DummyKVStorable> secondBucketElements = drumsAfterSplitting.read(1, 0, 50);

        Assert.assertEquals(50, firstBucketElements.size());
        Assert.assertEquals(50, secondBucketElements.size());

        for (int i = 0; i < firstBucketElements.size(); i++) {
            Assert.assertEquals(testData[i], firstBucketElements.get(i));
        }

        for (int i = 0; i < secondBucketElements.size(); i++) {
            Assert.assertEquals(testData[i + 50], secondBucketElements.get(i));
        }
    }

    /**
     * Generates a bucket with 100 elements and split the bucket into 4 ones.
     * 
     * @throws Exception
     */
    @Test
    public void oneBucket4Split() throws Exception {
        int numberOfElements = 100;
        RangeHashFunction hashFunction = RangeHashFunctionTestUtils.createTestFunction(1, 100, hashFunctionFilename,
                TestUtils.gp.keySize);

        DummyKVStorable[] testData = createAndFillDRUMS(numberOfElements, hashFunction);

        BucketSplitter<DummyKVStorable> splitter = new BucketSplitter<DummyKVStorable>(hashFunction,
                TestUtils.gp);
        splitter.splitAndStoreConfiguration(0, 4);

        DRUMS<DummyKVStorable> drumsAfterSplitting = DRUMSInstantiator.openTable(hashFunction,
                AccessMode.READ_ONLY, TestUtils.gp);
        // We must set the hash function because the hash function is loaded from the curious configuration file.
        drumsAfterSplitting.setHashFunction(hashFunction);

        List<DummyKVStorable> firstBucketElements = drumsAfterSplitting.read(0, 0, 50);
        List<DummyKVStorable> secondBucketElements = drumsAfterSplitting.read(1, 0, 50);
        List<DummyKVStorable> thirdBucketElements = drumsAfterSplitting.read(2, 0, 50);
        List<DummyKVStorable> fourthBucketElements = drumsAfterSplitting.read(3, 0, 50);

        Assert.assertEquals(25, firstBucketElements.size());
        Assert.assertEquals(25, secondBucketElements.size());
        Assert.assertEquals(25, thirdBucketElements.size());
        Assert.assertEquals(25, fourthBucketElements.size());

        for (int i = 0; i < firstBucketElements.size(); i++) {
            Assert.assertEquals(testData[i], firstBucketElements.get(i));
        }

        for (int i = 0; i < secondBucketElements.size(); i++) {
            Assert.assertEquals(testData[i + 25], secondBucketElements.get(i));
        }

        for (int i = 0; i < thirdBucketElements.size(); i++) {
            Assert.assertEquals(testData[i + 50], thirdBucketElements.get(i));
        }

        for (int i = 0; i < secondBucketElements.size(); i++) {
            Assert.assertEquals(testData[i + 75], fourthBucketElements.get(i));
        }
    }

    /**
     * Generates a bucket with 1.200.000 test data elements and splits them into 4 buckets.
     * 
     * @throws Exception
     */
    @Ignore
    @Test
    public void oneBigBucketSplit() throws Exception {
        int numberOfElements = 1200000;
        RangeHashFunction hashFunction = RangeHashFunctionTestUtils.createTestFunction(1, 2000000,
                hashFunctionFilename, TestUtils.gp.keySize);

        DummyKVStorable[] testData = createAndFillDRUMS(numberOfElements, hashFunction);

        BucketSplitter<DummyKVStorable> splitter = new BucketSplitter<DummyKVStorable>(hashFunction, TestUtils.gp);
        splitter.splitAndStoreConfiguration(0, 4);

        DRUMS<DummyKVStorable> drumsAfterSplitting = DRUMSInstantiator.openTable(hashFunction, AccessMode.READ_ONLY,
                TestUtils.gp);
        // We must set the hash function because the hash function is loaded from the curious configuration file.
        drumsAfterSplitting.setHashFunction(hashFunction);

        List<DummyKVStorable> firstBucketElements = drumsAfterSplitting.read(0, 0, 1000000);
        List<DummyKVStorable> secondBucketElements = drumsAfterSplitting.read(1, 0, 1000000);
        List<DummyKVStorable> thirdBucketElements = drumsAfterSplitting.read(2, 0, 1000000);
        List<DummyKVStorable> fourthBucketElements = drumsAfterSplitting.read(3, 0, 1000000);

        Assert.assertEquals(300000, firstBucketElements.size());
        Assert.assertEquals(300000, secondBucketElements.size());
        Assert.assertEquals(300000, thirdBucketElements.size());
        Assert.assertEquals(300000, fourthBucketElements.size());

        for (int i = 0; i < firstBucketElements.size(); i++) {
            Assert.assertEquals(testData[i], firstBucketElements.get(i));
        }

        for (int i = 0; i < secondBucketElements.size(); i++) {
            Assert.assertEquals(testData[i + 300000], secondBucketElements.get(i));
        }

        for (int i = 0; i < thirdBucketElements.size(); i++) {
            Assert.assertEquals(testData[i + 600000], thirdBucketElements.get(i));
        }

        for (int i = 0; i < secondBucketElements.size(); i++) {
            Assert.assertEquals(testData[i + 900000], fourthBucketElements.get(i));
        }
    }

    /**
     * Generates 2400 test elements, stores them into two buckets and the second bucket is split to four buckets.
     * 
     * @throws Exception
     */
    @Test
    public void splitSecondBucket() throws Exception {
        int numberOfElements = 2400;
        RangeHashFunction hashFunction = RangeHashFunctionTestUtils.createTestFunction(2, 1200, hashFunctionFilename,
                TestUtils.gp.keySize);

        DummyKVStorable[] testData = createAndFillDRUMS(numberOfElements, hashFunction);

        BucketSplitter<DummyKVStorable> splitter = new BucketSplitter<DummyKVStorable>(hashFunction,
                TestUtils.gp);
        splitter.splitAndStoreConfiguration(1, 4);

        DRUMS<DummyKVStorable> drumsAfterSplitting = DRUMSInstantiator.openTable(hashFunction,
                AccessMode.READ_ONLY, TestUtils.gp);
        // We must set the hash function because the hash function is loaded from the curious configuration file.
        drumsAfterSplitting.setHashFunction(hashFunction);

        List<DummyKVStorable> firstBucketElements = drumsAfterSplitting.read(1, 0, 1000000);
        List<DummyKVStorable> secondBucketElements = drumsAfterSplitting.read(2, 0, 1000000);
        List<DummyKVStorable> thirdBucketElements = drumsAfterSplitting.read(3, 0, 1000000);
        List<DummyKVStorable> fourthBucketElements = drumsAfterSplitting.read(4, 0, 1000000);

        Assert.assertEquals(300, firstBucketElements.size());
        Assert.assertEquals(300, secondBucketElements.size());
        Assert.assertEquals(300, thirdBucketElements.size());
        Assert.assertEquals(300, fourthBucketElements.size());

        for (int i = 0; i < firstBucketElements.size(); i++) {
            Assert.assertEquals(testData[1200 + i], firstBucketElements.get(i));
        }

        for (int i = 0; i < secondBucketElements.size(); i++) {
            Assert.assertEquals(testData[1200 + i + 300], secondBucketElements.get(i));
        }

        for (int i = 0; i < thirdBucketElements.size(); i++) {
            Assert.assertEquals(testData[1200 + i + 600], thirdBucketElements.get(i));
        }

        for (int i = 0; i < secondBucketElements.size(); i++) {
            Assert.assertEquals(testData[1200 + i + 900], fourthBucketElements.get(i));
        }
    }

    /**
     * Generates the specified number of test data and stores them into an DRUMS. The specified
     * <code>hashFunction</code> is used for that purpose. The test data that were stored within the DRUMS are returned.
     * 
     * @param numberOfData
     * @param hashFunction
     * @return
     * @throws Exception
     */
    private DummyKVStorable[] createAndFillDRUMS(int numberOfData, RangeHashFunction hashFunction) throws Exception {
        DummyKVStorable[] testData = TestUtils.generateTestdata(numberOfData);
        DRUMS<DummyKVStorable> drums = DRUMSInstantiator.createOrOpenTable(hashFunction, TestUtils.gp);
        drums.insertOrMerge(testData);
        drums.close();
        return testData;
    }
}
