package com.unister.semweb.sdrum.bucket;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.unister.semweb.sdrum.TestUtils;
import com.unister.semweb.sdrum.api.SDRUM;
import com.unister.semweb.sdrum.api.SDRUM.AccessMode;
import com.unister.semweb.sdrum.api.SDRUM_API;
import com.unister.semweb.sdrum.bucket.hashfunction.RangeHashFunction;
import com.unister.semweb.sdrum.file.FileLockException;
import com.unister.semweb.sdrum.storable.DummyKVStorable;
import com.unister.semweb.sdrum.utils.RangeHashFunctionTestUtils;

/**
 * This class is for testing the BucketSplitter The tests creates one database file and tries to split this file. Also
 * the test data are linear meaning that there is no gap between the keys within the data.
 * 
 * @author m.gleditzsch, n.thieme
 */
public class BucketSplitterTestLinear {
    private static final String hashFunctionFilename = "/tmp/hash.hs";

    @Before
    public void initialise() throws IOException, FileLockException {
        TestUtils.init();
        FileUtils.deleteQuietly(new File(TestUtils.gp.databaseDirectory));
    }
    @After
    public void close() {
        TestUtils.gp.index.close();
    }
    /**
     * Generates a bucket with 100 elements and splits this bucket into two ones.
     * 
     * @throws Exception
     */
    @Test
    public void oneBucket2Split() throws Exception {
        int numberOfElements = 100;
        RangeHashFunction hf = RangeHashFunctionTestUtils.createTestFunction(1, 100, hashFunctionFilename,
                TestUtils.gp.keySize);
        DummyKVStorable[] testData = createAndFillSDRUM(numberOfElements, hf);

        TestUtils.gp.openIndex(); // the index was closed
        
        BucketSplitter<DummyKVStorable> splitter = new BucketSplitter<DummyKVStorable>(hf, TestUtils.gp);
        splitter.splitAndStoreConfiguration(0, 2);
        SDRUM<DummyKVStorable> sdrumAfterSplitting = SDRUM_API.openTable(hf, AccessMode.READ_ONLY, TestUtils.gp);
        // We must set the hash function because the hash function is loaded from the curious configuration file.
        sdrumAfterSplitting.setHashFunction(hf);

        List<DummyKVStorable> firstBucketElements = sdrumAfterSplitting.read(0, 0, 50);
        List<DummyKVStorable> secondBucketElements = sdrumAfterSplitting.read(1, 0, 50);

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

        DummyKVStorable[] testData = createAndFillSDRUM(numberOfElements, hashFunction);

        BucketSplitter<DummyKVStorable> splitter = new BucketSplitter<DummyKVStorable>(hashFunction, TestUtils.gp);
        splitter.splitAndStoreConfiguration(0, 4);

        TestUtils.gp.openIndex(); // the index was closed
        SDRUM<DummyKVStorable> sdrumAfterSplitting = SDRUM_API.openTable(hashFunction,
                AccessMode.READ_ONLY, TestUtils.gp);
        // We must set the hash function because the hash function is loaded from the curious configuration file.
        sdrumAfterSplitting.setHashFunction(hashFunction);

        List<DummyKVStorable> firstBucketElements = sdrumAfterSplitting.read(0, 0, 50);
        List<DummyKVStorable> secondBucketElements = sdrumAfterSplitting.read(1, 0, 50);
        List<DummyKVStorable> thirdBucketElements = sdrumAfterSplitting.read(2, 0, 50);
        List<DummyKVStorable> fourthBucketElements = sdrumAfterSplitting.read(3, 0, 50);

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
     * Generates 2400 test elements, stores them into two buckets and the second bucket is split to four buckets.
     * 
     * @throws Exception
     */
    @Test
    public void splitSecondBucket() throws Exception {
        int numberOfElements = 2400;
        RangeHashFunction hashFunction = RangeHashFunctionTestUtils.createTestFunction(2, 1200, hashFunctionFilename,
                TestUtils.gp.keySize);

        DummyKVStorable[] testData = createAndFillSDRUM(numberOfElements, hashFunction);

        BucketSplitter<DummyKVStorable> splitter = new BucketSplitter<DummyKVStorable>(hashFunction, TestUtils.gp);
        splitter.splitAndStoreConfiguration(1, 4);

        SDRUM<DummyKVStorable> sdrumAfterSplitting = SDRUM_API.openTable(hashFunction,
                AccessMode.READ_ONLY, TestUtils.gp);
        // We must set the hash function because the hash function is loaded from the curious configuration file.
        sdrumAfterSplitting.setHashFunction(hashFunction);

        List<DummyKVStorable> firstBucketElements = sdrumAfterSplitting.read(1, 0, 1000000);
        List<DummyKVStorable> secondBucketElements = sdrumAfterSplitting.read(2, 0, 1000000);
        List<DummyKVStorable> thirdBucketElements = sdrumAfterSplitting.read(3, 0, 1000000);
        List<DummyKVStorable> fourthBucketElements = sdrumAfterSplitting.read(4, 0, 1000000);

        Assert.assertEquals(300, firstBucketElements.size());
        Assert.assertEquals(300, secondBucketElements.size());
        Assert.assertEquals(300, thirdBucketElements.size());
        Assert.assertEquals(300, fourthBucketElements.size());

        Assert.assertArrayEquals(Arrays.copyOfRange(testData, 1200, 1500),
                firstBucketElements.toArray(new DummyKVStorable[firstBucketElements.size()]));
        Assert.assertArrayEquals(Arrays.copyOfRange(testData, 1500, 1800),
                secondBucketElements.toArray(new DummyKVStorable[secondBucketElements.size()]));
        Assert.assertArrayEquals(Arrays.copyOfRange(testData, 1800, 2100),
                thirdBucketElements.toArray(new DummyKVStorable[thirdBucketElements.size()]));
        Assert.assertArrayEquals(Arrays.copyOfRange(testData, 2100, 2400),
                fourthBucketElements.toArray(new DummyKVStorable[fourthBucketElements.size()]));
    }

    /**
     * Generates the specified number of test data and stores them into an SDRUM. The specified
     * <code>hashFunction</code> is used for that purpose. The test data that were stored within the SDRUM are returned.
     * 
     * @param numberOfData
     * @param hashFunction
     * @return
     * @throws Exception
     */
    private DummyKVStorable[] createAndFillSDRUM(int numberOfData, RangeHashFunction hashFunction) throws Exception {
        DummyKVStorable[] testData = TestUtils.generateTestdata(numberOfData);
        SDRUM<DummyKVStorable> sdrum = SDRUM_API.createOrOpenTable(hashFunction, TestUtils.gp);
        sdrum.insertOrMerge(testData);
        sdrum.close();
        return testData;
    }
}
