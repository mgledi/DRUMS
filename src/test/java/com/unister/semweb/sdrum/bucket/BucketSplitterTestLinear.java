package com.unister.semweb.sdrum.bucket;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.junit.Assert;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import com.unister.semweb.sdrum.TestUtils;
import com.unister.semweb.sdrum.api.SDRUM;
import com.unister.semweb.sdrum.api.SDRUM.AccessMode;
import com.unister.semweb.sdrum.api.SDRUM_API;
import com.unister.semweb.sdrum.bucket.hashfunction.RangeHashFunction;
import com.unister.semweb.sdrum.storable.DummyKVStorable;
import com.unister.semweb.sdrum.utils.RangeHashFunctionTestUtils;

/**
 * This class is for testing the BucketSplitter The tests creates one database file and tries to split this file. Also
 * the test data are linear meaning that there is no gap between the keys within the data.
 * 
 * @author m.gleditzsch, n.thieme
 */
public class BucketSplitterTestLinear {
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
     * Generates a bucket with 100 elements and splits this bucket into two ones.
     * 
     * @throws Exception
     */
    @Test
    public void oneBucket2Split() throws Exception {
        int numberOfElements = 100;
        RangeHashFunction hashFunction = RangeHashFunctionTestUtils.createTestFunction(1, 100,
                hashFunctionFilename, prototypeKeySize);
        DummyKVStorable[] testData = createAndFillSDRUM(numberOfElements, hashFunction);

        BucketSplitter<DummyKVStorable> splitter = new BucketSplitter<DummyKVStorable>(databaseDirectory, hashFunction,
                prototype);
        splitter.splitAndStoreConfiguration(0, 2);

        SDRUM<DummyKVStorable> sdrumAfterSplitting = SDRUM_API.openTable(databaseDirectory, AccessMode.READ_ONLY,
                prototype);
        // We must set the hash function because the hash function is loaded from the curious configuration file.
        sdrumAfterSplitting.setHashFunction(hashFunction);

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
                prototypeKeySize);

        DummyKVStorable[] testData = createAndFillSDRUM(numberOfElements, hashFunction);

        BucketSplitter<DummyKVStorable> splitter = new BucketSplitter<DummyKVStorable>(databaseDirectory, hashFunction,
                prototype);
        splitter.splitAndStoreConfiguration(0, 4);

        SDRUM<DummyKVStorable> sdrumAfterSplitting = SDRUM_API.openTable(databaseDirectory, AccessMode.READ_ONLY,
                prototype);
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
     * Generates a bucket with 1.200.000 test data elements and splits them into 4 buckets.
     * 
     * @throws Exception
     */
    @Test
    public void oneBigBucketSplit() throws Exception {
        int numberOfElements = 1200000;
        RangeHashFunction hashFunction = RangeHashFunctionTestUtils.createTestFunction(1, 2000000,
                hashFunctionFilename, prototypeKeySize);

        DummyKVStorable[] testData = createAndFillSDRUM(numberOfElements, hashFunction);

        BucketSplitter<DummyKVStorable> splitter = new BucketSplitter<DummyKVStorable>(databaseDirectory, hashFunction,
                prototype);
        splitter.splitAndStoreConfiguration(0, 4);

        SDRUM<DummyKVStorable> sdrumAfterSplitting = SDRUM_API.openTable(databaseDirectory, AccessMode.READ_ONLY,
                prototype);
        // We must set the hash function because the hash function is loaded from the curious configuration file.
        sdrumAfterSplitting.setHashFunction(hashFunction);

        List<DummyKVStorable> firstBucketElements = sdrumAfterSplitting.read(0, 0, 1000000);
        List<DummyKVStorable> secondBucketElements = sdrumAfterSplitting.read(1, 0, 1000000);
        List<DummyKVStorable> thirdBucketElements = sdrumAfterSplitting.read(2, 0, 1000000);
        List<DummyKVStorable> fourthBucketElements = sdrumAfterSplitting.read(3, 0, 1000000);

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
                prototypeKeySize);

        DummyKVStorable[] testData = createAndFillSDRUM(numberOfElements, hashFunction);

        BucketSplitter<DummyKVStorable> splitter = new BucketSplitter<DummyKVStorable>(databaseDirectory, hashFunction,
                prototype);
        splitter.splitAndStoreConfiguration(1, 4);

        SDRUM<DummyKVStorable> sdrumAfterSplitting = SDRUM_API.openTable(databaseDirectory, AccessMode.READ_ONLY,
                prototype);
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
        SDRUM<DummyKVStorable> sdrum = SDRUM_API.createOrOpenTable(databaseDirectory, 10000, 1, hashFunction,
                prototype);
        sdrum.insertOrMerge(testData);
        sdrum.close();
        return testData;
    }

    // @Before
    // public void initialise() throws Exception {
    // long[] ranges = new long[] { 1, 10, 100, 1000 };
    // byte[][] bRanges = KeyUtils.transformToByteArray(ranges);
    // String[] filenames = new String[] { "1.db", "2", "3.db", "4.db" };
    // int[] sizes = { 1000, 1000, 1000, 1000 };
    // FileUtils.deleteQuietly(new File(databaseDirectory));
    //
    // this.hashFunction = new RangeHashFunction(bRanges, filenames, sizes, new File(hashFunctionFilename));
    // this.prototype = new DummyKVStorable();
    //
    // // fill with data
    // startSDRUM(hashFunction);
    // testData = TestUtils.createDummyData(-100, 1500);
    // table.insertOrMerge(testData);
    // stopSDRUM();
    // }
    //
    // private void startSDRUM(RangeHashFunction hashFunction) throws Exception {
    // table = SDRUM_API.createOrOpenTable(databaseDirectory, 1000,
    // 10000, 1, hashFunction, prototype);
    // table.setHashFunction(hashFunction);
    // }
    //
    // private void stopSDRUM() throws Exception {
    // table.close();
    // }
    //
    // @Test
    // public void generateFileName() throws IOException, FileLockException {
    // BucketSplitter<DummyKVStorable> bs = new BucketSplitter<DummyKVStorable>(databaseDirectory, this.hashFunction,
    // 3, 3);
    // Assert.assertEquals(bs.generateFileName(1, hashFunction.getFilename(3)), "4_1.db");
    // Assert.assertEquals(bs.generateFileName(1, hashFunction.getFilename(1)), "2_1");
    // }
    //
    // @Test
    // public void storeHashFunction() {
    // fail("not written yet");
    // }
    //
    // @Test
    // public void moveElements() throws Exception {
    // new BucketSplitter<DummyKVStorable>(databaseDirectory, this.hashFunction, 2, 4);
    //
    // RangeHashFunction reReadFunction = new RangeHashFunction(new File(hashFunctionFilename));
    // startSDRUM(reReadFunction);
    //
    // List<DummyKVStorable> storedElementNewBucket1 = table.read(2, 0, 10000);
    // List<DummyKVStorable> storedElementNewBucket2 = table.read(3, 0, 10000);
    // List<DummyKVStorable> storedElementNewBucket3 = table.read(4, 0, 10000);
    // List<DummyKVStorable> storedElementNewBucket4 = table.read(5, 0, 10000);
    // stopSDRUM();
    //
    // Set<DummyKVStorable> mergedData = merge(storedElementNewBucket1, storedElementNewBucket2,
    // storedElementNewBucket3, storedElementNewBucket4);
    //
    // DummyKVStorable[] distributedData = TestUtils.createDummyData(10, 100);
    // boolean areEqual = areEqual(distributedData, mergedData);
    // Assert.assertTrue(areEqual);
    // }
    //
    // private Set<DummyKVStorable> merge(List<DummyKVStorable>... toMerge) {
    // Set<DummyKVStorable> result = new HashSet<DummyKVStorable>();
    // for (List<DummyKVStorable> toAdd : toMerge) {
    // result.addAll(toAdd);
    // }
    // return result;
    // }
    //
    // private boolean areEqual(DummyKVStorable[] first, Set<DummyKVStorable> second) {
    // List<DummyKVStorable> converted = Arrays.asList(first);
    // Set<DummyKVStorable> convertedSet = new HashSet<DummyKVStorable>(converted);
    // if (second.equals(convertedSet)) {
    // return true;
    // } else {
    // return false;
    // }
    // }
    //
    // @Test
    // public void deleteOldFile() {
    // fail("not written yet");
    // }
}
