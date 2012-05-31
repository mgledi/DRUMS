package com.unister.semweb.sdrum.bucket;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
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

    @Before
    public void initialise() throws IOException {
        FileUtils.deleteQuietly(new File(sdrumDirectory));
        new File(sdrumDirectory).mkdirs();
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
        RangeHashFunction hashFunction = RangeHashFunctionTestUtils.createTestFunction(1, 10000, hashFunctionFilename,
                TestUtils.gp.keySize);
        DummyKVStorable[] testData = createAndFillSDRUM(10, 10, hashFunction);

        BucketSplitter<DummyKVStorable> splitter =
                new BucketSplitter<DummyKVStorable>(databaseDirectory, hashFunction, TestUtils.gp);
        splitter.splitAndStoreConfiguration(0, 2);

        SDRUM<DummyKVStorable> sdrumAfterSplitting =
                SDRUM_API.openTable(databaseDirectory, hashFunction, AccessMode.READ_ONLY, TestUtils.gp);
        // We must set the hash function because the hash function is loaded from the curious configuration file.
        sdrumAfterSplitting.setHashFunction(hashFunction);

        List<DummyKVStorable> firstBucketElements = sdrumAfterSplitting.read(0, 0, 100);
        List<DummyKVStorable> secondBucketElements = sdrumAfterSplitting.read(1, 0, 100);

        Assert.assertEquals(5, firstBucketElements.size());
        Assert.assertEquals(5, secondBucketElements.size());

        Assert.assertArrayEquals(Arrays.copyOfRange(testData, 0, 5),
                firstBucketElements.toArray(new DummyKVStorable[firstBucketElements.size()]));
        Assert.assertArrayEquals(Arrays.copyOfRange(testData, 5, 10),
                secondBucketElements.toArray(new DummyKVStorable[firstBucketElements.size()]));

        byte[][] expectedRanges = new byte[][] { KeyUtils.convert(41), KeyUtils.convert(10000) };
        Assert.assertArrayEquals(expectedRanges, hashFunction.getRanges());
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
                hashFunctionFilename, TestUtils.gp.keySize);
        DummyKVStorable[] testData = createAndFillSDRUM(numberOfElements, 100, hashFunction);

        BucketSplitter<DummyKVStorable> splitter = new BucketSplitter<DummyKVStorable>(databaseDirectory, hashFunction,
                TestUtils.gp);
        splitter.splitAndStoreConfiguration(0, 2);

        SDRUM<DummyKVStorable> sdrumAfterSplitting = SDRUM_API.openTable(databaseDirectory, hashFunction,
                AccessMode.READ_ONLY, TestUtils.gp);
        // We must set the hash function because the hash function is loaded from the curious configuration file.
        sdrumAfterSplitting.setHashFunction(hashFunction);

        List<DummyKVStorable> firstBucketElements = sdrumAfterSplitting.read(0, 0, 500000);
        List<DummyKVStorable> secondBucketElements = sdrumAfterSplitting.read(1, 0, 500000);

        Assert.assertEquals(2500, firstBucketElements.size());
        Assert.assertEquals(2500, secondBucketElements.size());

        Assert.assertArrayEquals(Arrays.copyOfRange(testData, 0, 2500),
                firstBucketElements.toArray(new DummyKVStorable[firstBucketElements.size()]));
        Assert.assertArrayEquals(Arrays.copyOfRange(testData, 2500, 5000),
                secondBucketElements.toArray(new DummyKVStorable[firstBucketElements.size()]));

        byte[][] expectedRanges = KeyUtils.transformToByteArray(new long[]{249901, 500000 });
        Assert.assertArrayEquals(expectedRanges, hashFunction.getRanges());
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
                hashFunctionFilename, TestUtils.gp.keySize);
        DummyKVStorable[] testData = createAndFillSDRUM(numberOfElements, 100, hashFunction);

        BucketSplitter<DummyKVStorable> splitter = new BucketSplitter<DummyKVStorable>(databaseDirectory, hashFunction,
                TestUtils.gp);
        splitter.splitAndStoreConfiguration(0, 4);

        SDRUM<DummyKVStorable> sdrumAfterSplitting = SDRUM_API.openTable(databaseDirectory, hashFunction,
                AccessMode.READ_ONLY, TestUtils.gp);
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

        Assert.assertArrayEquals(Arrays.copyOfRange(testData, 0, 1250),
                firstBucketElements.toArray(new DummyKVStorable[firstBucketElements.size()]));
        Assert.assertArrayEquals(Arrays.copyOfRange(testData, 1250, 2500),
                secondBucketElements.toArray(new DummyKVStorable[secondBucketElements.size()]));
        Assert.assertArrayEquals(Arrays.copyOfRange(testData, 2500, 3750),
                thirdBucketElements.toArray(new DummyKVStorable[thirdBucketElements.size()]));
        Assert.assertArrayEquals(Arrays.copyOfRange(testData, 3750, 5000),
                fourthBucketElements.toArray(new DummyKVStorable[fourthBucketElements.size()]));


        byte[][] expectedRanges = KeyUtils.transformToByteArray(new long[]{124901, 249901, 374901, 500000});                
        Assert.assertArrayEquals(expectedRanges, hashFunction.getRanges());
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
        SDRUM<DummyKVStorable> sdrum = SDRUM_API.createOrOpenTable(databaseDirectory, hashFunction, TestUtils.gp);
        sdrum.insertOrMerge(testData);
        sdrum.close();
        return testData;
    }
}
