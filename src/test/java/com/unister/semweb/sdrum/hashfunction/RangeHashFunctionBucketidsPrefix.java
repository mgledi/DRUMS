package com.unister.semweb.sdrum.hashfunction;

import java.io.File;

import junit.framework.Assert;

import org.junit.Test;

import com.unister.semweb.sdrum.bucket.hashfunction.RangeHashFunction;

/**
 * Tests the method <code>getBucketIds()</code> from the {@link RangeHashFunction}.
 * 
 * @author n.thieme
 * 
 */
public class RangeHashFunctionBucketidsPrefix {
    private static final String HASHFUNCTION_FILENAME = "/tmp/prefixGetRangeHashFunction.txt";
    private static final int BUCKET_SIZE = 100;

    /**
     * Some maximal ranges are added to the hash function and only one range of these are returned.
     */
    @Test
    public void oneBucket() {
        byte[][] rangeValues = new byte[][] { { 0, 0, 1, 0 }, { 0, 0, 5, 0 }, { 0, 0, 6, 0 }, { 0, 0, 7, 0 } };
        String[] generatedFilenames = generateFilenames(rangeValues.length);
        int[] bucketSizes = generateBucketSizes(rangeValues.length, BUCKET_SIZE);

        RangeHashFunction hashFunction = new RangeHashFunction(rangeValues, generatedFilenames, bucketSizes, new File(
                HASHFUNCTION_FILENAME));

        byte[] prefix = { 0, 0, 2 };
        int[] bucketIds = hashFunction.getBucketIdsFor(prefix);
        Assert.assertEquals(1, bucketIds.length);
        Assert.assertEquals(1, bucketIds[0]);
    }

    /**
     * We search for all bucket ids that confirm to the given prefix. In this test two bucket ids will be returned.
     */
    @Test
    public void twoBuckets() {
        byte[][] rangeValues = new byte[][] { { 0, 0, 1, 0 }, { 0, 0, 2, 0 }, { 0, 0, 3, 0 }, { 0, 0, 4, 0 } };
        String[] generatedFilenames = generateFilenames(rangeValues.length);
        int[] bucketSizes = generateBucketSizes(rangeValues.length, BUCKET_SIZE);

        RangeHashFunction hashFunction = new RangeHashFunction(rangeValues, generatedFilenames, bucketSizes, new File(
                HASHFUNCTION_FILENAME));

        byte[] prefix = { 0, 0, 1 };
        int[] bucketIds = hashFunction.getBucketIdsFor(prefix);
        Assert.assertEquals(2, bucketIds.length);
        Assert.assertEquals(0, bucketIds[0]);
        Assert.assertEquals(1, bucketIds[1]);
    }

    /**
     * We search for the bucket ids that confirm to the given prefix. In this test three buckets ids will be returned.
     */
    @Test
    public void twoBucketsInTheMiddle() {
        byte[][] rangeValues = new byte[][] { { 0, 0, 1, 0 }, { 0, 0, 1, 5 }, { 0, 0, 2, 1 }, { 0, 0, 2, 2 },
                { 0, 0, 3, 0 }, { 0, 0, 4, 0 } };
        String[] generatedFilenames = generateFilenames(rangeValues.length);
        int[] bucketSizes = generateBucketSizes(rangeValues.length, BUCKET_SIZE);

        RangeHashFunction hashFunction = new RangeHashFunction(rangeValues, generatedFilenames, bucketSizes, new File(
                HASHFUNCTION_FILENAME));

        byte[] prefix = { 0, 0, 2 };
        int[] bucketIds = hashFunction.getBucketIdsFor(prefix);
        Assert.assertEquals(3, bucketIds.length);
        Assert.assertEquals(2, bucketIds[0]);
        Assert.assertEquals(3, bucketIds[1]);
        Assert.assertEquals(4, bucketIds[2]);
    }

    /**
     * We search for the bucket ids that confirm to the given prefix. In this test the last two buckets ids will be
     * returned.
     */
    @Test
    public void twoBucketsAtTheEnd() {
        byte[][] rangeValues = new byte[][] { { 0, 0, 1, 0 }, { 0, 0, 1, 5 }, { 0, 0, 2, 1 }, { 0, 0, 2, 2 },
                { 0, 0, 3, 0 }, { 0, 0, 4, 0 } };
        String[] generatedFilenames = generateFilenames(rangeValues.length);
        int[] bucketSizes = generateBucketSizes(rangeValues.length, BUCKET_SIZE);

        RangeHashFunction hashFunction = new RangeHashFunction(rangeValues, generatedFilenames, bucketSizes, new File(
                HASHFUNCTION_FILENAME));

        byte[] prefix = { 0, 0, 3 };
        int[] bucketIds = hashFunction.getBucketIdsFor(prefix);
        Assert.assertEquals(2, bucketIds.length);
        Assert.assertEquals(4, bucketIds[0]);
        Assert.assertEquals(5, bucketIds[1]);
    }

    /**
     * We search for the bucket ids that confirm to the given prefix. In this test the last and the bucket ids will be
     * returned.
     */
    @Test
    public void twoBucketsAtTheEnd2() {
        byte[][] rangeValues = new byte[][] { { 0, 0, 1, 0 }, { 0, 0, 1, 5 }, { 0, 0, 2, 1 }, { 0, 0, 2, 2 },
                { 0, 0, 3, 0 }, { 0, 0, 4, 0 } };
        String[] generatedFilenames = generateFilenames(rangeValues.length);
        int[] bucketSizes = generateBucketSizes(rangeValues.length, BUCKET_SIZE);

        RangeHashFunction hashFunction = new RangeHashFunction(rangeValues, generatedFilenames, bucketSizes, new File(
                HASHFUNCTION_FILENAME));

        byte[] prefix = { 0, 0, 4 };
        int[] bucketIds = hashFunction.getBucketIdsFor(prefix);
        Assert.assertEquals(2, bucketIds.length);
        Assert.assertEquals(5, bucketIds[0]);
        Assert.assertEquals(0, bucketIds[1]);
    }

    /**
     * The prefix that we use has more bytes than the max range arrays within the {@link RangeHashFunction}. So we
     * expect an {@link IllegalArgumentException}.
     */
    @Test(expected = IllegalArgumentException.class)
    public void greaterPrefix() {
        byte[][] rangeValues = new byte[][] { { 0, 0, 1, 0 }, { 0, 0, 1, 5 }, { 0, 0, 2, 1 }, { 0, 0, 2, 2 },
                { 0, 0, 3, 0 }, { 0, 0, 4, 0 } };
        String[] generatedFilenames = generateFilenames(rangeValues.length);
        int[] bucketSizes = generateBucketSizes(rangeValues.length, BUCKET_SIZE);

        RangeHashFunction hashFunction = new RangeHashFunction(rangeValues, generatedFilenames, bucketSizes, new File(
                HASHFUNCTION_FILENAME));

        byte[] prefix = { 0, 0, 2, 1, 1 };
        int[] bucketIds = hashFunction.getBucketIdsFor(prefix);
        Assert.fail();
    }

    /**
     * Generate the specified <code>numberOfFiles</code> file names.
     * 
     * @param numberOfFiles
     * @return
     */
    private String[] generateFilenames(int numberOfFiles) {
        String[] result = new String[numberOfFiles];
        for (int i = 0; i < numberOfFiles; i++) {
            String bucketFilename = "bucket" + i + ".db";
            result[i] = bucketFilename;
        }
        return result;
    }

    /**
     * Generates an array of bucket sizes. There <code>numberOfSizes</code> values. All have the value <code>size</code>
     * .
     * 
     * @param numberOfSizes
     * @param size
     * @return
     */
    private int[] generateBucketSizes(int numberOfSizes, int size) {
        int[] result = new int[numberOfSizes];
        for (int i = 0; i < numberOfSizes; i++) {
            result[i] = size;
        }
        return result;
    }
}
