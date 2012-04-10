package com.unister.semweb.sdrum.bucket;

import java.io.File;
import java.io.IOException;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import com.unister.semweb.sdrum.TestUtils;
import com.unister.semweb.sdrum.api.SDRUM;
import com.unister.semweb.sdrum.api.SDRUM.AccessMode;
import com.unister.semweb.sdrum.api.SDRUM_API;
import com.unister.semweb.sdrum.bucket.hashfunction.RangeHashFunction;
import com.unister.semweb.sdrum.storable.DummyKVStorable;
import com.unister.semweb.sdrum.utils.KeyUtils;
import com.unister.semweb.sdrum.utils.RangeHashFunctionTestUtils;

/**
 * Tests the {@link BucketSplitter} with non linear keys meaning that the keys within the buckets are not contiguous.
 * 
 * @author n.thieme
 */
public class BucketSplitterTestNonLinear {
    private static final String sdrumDirectory = "/tmp/bucketSplitting";
    private static final String databaseDirectory = sdrumDirectory + "/db";

    private static final String hashFunctionFilename = "/tmp/hash.hs";

    private DummyKVStorable prototype;
    private int prototypeKeySize;

    @Before
    public void initialise() throws IOException {
        FileUtils.deleteQuietly(new File(sdrumDirectory));
        new File(sdrumDirectory).mkdirs();
        prototype = new DummyKVStorable();
        prototypeKeySize = prototype.keySize;
    }

    /**
     * Generates 10 test element with a width of 10 (keys are 1, 11, 21, 31, 41, 51, 61, 71, 81 and 91).
     * The elements are stored with the SDRUM. The bucket is split into two halves. So there will be two buckets with
     * the following elements:
     * <ul>
     * <li>First bucket: 1, 11, 21, 31, 41</li>
     * <li>Second bucket: 51, 61, 71, 81, 91</li>
     * </ul>
     * 
     * @throws Exception
     */
    @Test
    public void oneBucket2Split10Elements10Width() throws Exception {
        int numberOfElements = 10;
        RangeHashFunction hashFunction = RangeHashFunctionTestUtils.createTestFunction(1, 10000,
                hashFunctionFilename, prototypeKeySize);
        DummyKVStorable[] testData = createAndFillSDRUM(numberOfElements, 10, hashFunction);

        BucketSplitter<DummyKVStorable> splitter = new BucketSplitter<DummyKVStorable>(databaseDirectory, hashFunction,
                prototype);
        splitter.splitAndStoreConfiguration(0, 2);

        SDRUM<DummyKVStorable> sdrumAfterSplitting = SDRUM_API.openTable(databaseDirectory, AccessMode.READ_ONLY,
                prototype);
        // We must set the hash function because the hash function is loaded from the curious configuration file.
        sdrumAfterSplitting.setHashFunction(hashFunction);

        List<DummyKVStorable> firstBucketElements = sdrumAfterSplitting.read(0, 0, 100);
        List<DummyKVStorable> secondBucketElements = sdrumAfterSplitting.read(1, 0, 100);

        Assert.assertEquals(5, firstBucketElements.size());
        Assert.assertEquals(5, secondBucketElements.size());

        for (int i = 0; i < firstBucketElements.size(); i++) {
            Assert.assertEquals(testData[i], firstBucketElements.get(i));
        }

        for (int i = 0; i < secondBucketElements.size(); i++) {
            Assert.assertEquals(testData[i + 5], secondBucketElements.get(i));
        }

        byte[][] expectedRanges = new byte[][] { KeyUtils.transformFromLong(41, prototypeKeySize),
                KeyUtils.transformFromLong(10000, prototypeKeySize) };
        Assert.assertTrue(examineHashFunction(hashFunction, expectedRanges));
    }

    /**
     * Creates 10 test data elements with a width of 100. Then the test data are written to the sdrum and the bucket is
     * split into two halves.
     * 
     * @throws Exception
     */
    @Test
    public void oneBucket2Split10Elements100Width() throws Exception {
        int numberOfElements = 10;
        RangeHashFunction hashFunction = RangeHashFunctionTestUtils.createTestFunction(1, 10000,
                hashFunctionFilename, prototypeKeySize);
        DummyKVStorable[] testData = createAndFillSDRUM(numberOfElements, 100, hashFunction);

        BucketSplitter<DummyKVStorable> splitter = new BucketSplitter<DummyKVStorable>(databaseDirectory, hashFunction,
                prototype);
        splitter.splitAndStoreConfiguration(0, 2);

        SDRUM<DummyKVStorable> sdrumAfterSplitting = SDRUM_API.openTable(databaseDirectory, AccessMode.READ_ONLY,
                prototype);
        // We must set the hash function because the hash function is loaded from the curious configuration file.
        sdrumAfterSplitting.setHashFunction(hashFunction);

        List<DummyKVStorable> firstBucketElements = sdrumAfterSplitting.read(0, 0, 100);
        List<DummyKVStorable> secondBucketElements = sdrumAfterSplitting.read(1, 0, 100);

        Assert.assertEquals(5, firstBucketElements.size());
        Assert.assertEquals(5, secondBucketElements.size());

        for (int i = 0; i < firstBucketElements.size(); i++) {
            Assert.assertEquals(testData[i], firstBucketElements.get(i));
        }

        for (int i = 0; i < secondBucketElements.size(); i++) {
            Assert.assertEquals(testData[i + 5], secondBucketElements.get(i));
        }

        byte[][] expectedRanges = new byte[][] { KeyUtils.transformFromLong(401, prototypeKeySize),
                KeyUtils.transformFromLong(10000, prototypeKeySize) };
        Assert.assertTrue(examineHashFunction(hashFunction, expectedRanges));
    }

    /**
     * Creates 5000 test data elements with a width of 100. Then the test data are written to the sdrum and the bucket
     * is split into two halves.
     * 
     * @throws Exception
     */
    @Test
    public void oneBucket2Split5000Elements100Width() throws Exception {
        int numberOfElements = 5000;
        RangeHashFunction hashFunction = RangeHashFunctionTestUtils.createTestFunction(1, 500000,
                hashFunctionFilename, prototypeKeySize);
        DummyKVStorable[] testData = createAndFillSDRUM(numberOfElements, 100, hashFunction);

        BucketSplitter<DummyKVStorable> splitter = new BucketSplitter<DummyKVStorable>(databaseDirectory, hashFunction,
                prototype);
        splitter.splitAndStoreConfiguration(0, 2);

        SDRUM<DummyKVStorable> sdrumAfterSplitting = SDRUM_API.openTable(databaseDirectory, AccessMode.READ_ONLY,
                prototype);
        // We must set the hash function because the hash function is loaded from the curious configuration file.
        sdrumAfterSplitting.setHashFunction(hashFunction);

        List<DummyKVStorable> firstBucketElements = sdrumAfterSplitting.read(0, 0, 500000);
        List<DummyKVStorable> secondBucketElements = sdrumAfterSplitting.read(1, 0, 500000);

        Assert.assertEquals(2500, firstBucketElements.size());
        Assert.assertEquals(2500, secondBucketElements.size());

        for (int i = 0; i < firstBucketElements.size(); i++) {
            Assert.assertEquals(testData[i], firstBucketElements.get(i));
        }

        for (int i = 0; i < secondBucketElements.size(); i++) {
            Assert.assertEquals(testData[i + 2500], secondBucketElements.get(i));
        }

        byte[][] expectedRanges = new byte[][] { KeyUtils.transformFromLong(249901, prototypeKeySize),
                KeyUtils.transformFromLong(500000, prototypeKeySize) };
        Assert.assertTrue(examineHashFunction(hashFunction, expectedRanges));
    }

    /**
     * Creates 5000 test data elements with a width of 100. Then the test data are written to the sdrum and the bucket
     * is split into four halves.
     * 
     * @throws Exception
     */
    @Test
    public void oneBucket4Split5000Elements100Width() throws Exception {
        int numberOfElements = 5000;
        RangeHashFunction hashFunction = RangeHashFunctionTestUtils.createTestFunction(1, 500000,
                hashFunctionFilename, prototypeKeySize);
        DummyKVStorable[] testData = createAndFillSDRUM(numberOfElements, 100, hashFunction);

        BucketSplitter<DummyKVStorable> splitter = new BucketSplitter<DummyKVStorable>(databaseDirectory, hashFunction,
                prototype);
        splitter.splitAndStoreConfiguration(0, 4);

        SDRUM<DummyKVStorable> sdrumAfterSplitting = SDRUM_API.openTable(databaseDirectory, AccessMode.READ_ONLY,
                prototype);
        // We must set the hash function because the hash function is loaded from the curious configuration file.
        sdrumAfterSplitting.setHashFunction(hashFunction);

        List<DummyKVStorable> firstBucketElements = sdrumAfterSplitting.read(0, 0, 500000);
        List<DummyKVStorable> secondBucketElements = sdrumAfterSplitting.read(1, 0, 500000);
        List<DummyKVStorable> thirdBucketElements = sdrumAfterSplitting.read(2, 0, 500000);
        List<DummyKVStorable> fourthBucketElements = sdrumAfterSplitting.read(3, 0, 500000);

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

        byte[][] expectedRanges = new byte[][] { KeyUtils.transformFromLong(124901, prototypeKeySize),
                KeyUtils.transformFromLong(249901, prototypeKeySize),
                KeyUtils.transformFromLong(374901, prototypeKeySize),
                KeyUtils.transformFromLong(500000, prototypeKeySize) };
        Assert.assertTrue(examineHashFunction(hashFunction, expectedRanges));
    }

    /**
     * Generates the specified number of test data with the specified width and stores them into an SDRUM. The specified
     * <code>hashFunction</code> is used for that purpose. The test data that were stored within the SDRUM are returned.
     * 
     * @param numberOfData
     * @param hashFunction
     * @param width
     * @return
     * @throws Exception
     */
    private DummyKVStorable[] createAndFillSDRUM(int numberOfData, int width, RangeHashFunction hashFunction)
            throws Exception {
        DummyKVStorable[] testData = TestUtils.generateTestdata(numberOfData, width);
        SDRUM<DummyKVStorable> sdrum = SDRUM_API.createOrOpenTable(databaseDirectory, 1, hashFunction,
                prototype);
        sdrum.insertOrMerge(testData);
        sdrum.close();
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
