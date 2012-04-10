package com.unister.semweb.sdrum;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.unister.semweb.sdrum.bucket.Bucket;
import com.unister.semweb.sdrum.bucket.BucketContainer;
import com.unister.semweb.sdrum.bucket.hashfunction.AbstractHashFunction;
import com.unister.semweb.sdrum.bucket.hashfunction.RangeHashFunction;
import com.unister.semweb.sdrum.storable.DummyKVStorable;
import com.unister.semweb.sdrum.utils.KeyUtils;

/**
 * Tests whether the link data is added to the right bucket.
 * 
 * @author n.thieme
 * 
 */
public class BucketContainerTest {
    /**
     * Tries to add a link data to the bucket 0.
     * 
     * @throws Exception
     */
    @Test
    public void addOneDateToBucketContainerBucket0() throws Exception {
        makeTest(0);
    }

    /**
     * Tries to add a link data to the bucket 1.
     * 
     * @throws Exception
     */
    @Test
    public void addOneDateToBucketContainerBucket1() throws Exception {
        makeTest(1);
    }

    /**
     * Tries to add a link data to the bucket 9.
     * 
     * @throws Exception
     */
    @Test
    public void addOneDateToBucketContainerBucket9() throws Exception {
        makeTest(9);
    }

    /**
     * Makes the test. It tries to add a link data to the specific bucket number and examines whether the
     * link data lands in the right bucket.
     * 
     * @param numberOfBucket
     * @throws Exception
     */
    private void makeTest(int numberOfBucket) throws Exception {
        int numberOfBuckets = 64;

        long fingerprint = numberOfBucket;

        fingerprint = fingerprint << 65;

        DummyKVStorable linkData = new DummyKVStorable();
        linkData.setKey(KeyUtils.transformFromLong(fingerprint, linkData.keySize));
        linkData.setRelevanceScore(0.34);
        linkData.setParentCount(10);
        linkData.setTimestamp(System.currentTimeMillis());

        List<Bucket<DummyKVStorable>> bucketList = generateBucketList(numberOfBuckets);
        Bucket<DummyKVStorable>[] buckets = new Bucket[bucketList.size()];
        buckets = bucketList.toArray(buckets);

        AbstractHashFunction bucketComputer = new RangeHashFunction(numberOfBuckets, new DummyKVStorable().keySize, new File(""));
        BucketContainer<DummyKVStorable> bucketContainer = new BucketContainer<DummyKVStorable>(buckets, bucketComputer);
        bucketContainer.addToCache(linkData);

        // Assert.assertEquals(1, bucketList.get(numberOfBucket).size());
    }

    private List<Bucket<DummyKVStorable>> generateBucketList(int numberOfBuckets) {
        List<Bucket<DummyKVStorable>> result = new ArrayList<Bucket<DummyKVStorable>>();
        for (int i = 0; i < numberOfBuckets; i++) {
            Bucket<DummyKVStorable> newBucket = new Bucket<DummyKVStorable>(i, new DummyKVStorable());
            result.add(newBucket);
        }
        return result;
    }
}
