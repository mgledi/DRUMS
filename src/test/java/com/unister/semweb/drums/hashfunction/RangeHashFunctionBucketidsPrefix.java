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

/**
 * Tests the method <code>getBucketIds()</code> from the {@link RangeHashFunction}.
 * 
 * @author Nils Thieme
 * 
 */
public class RangeHashFunctionBucketidsPrefix {
    private static final String HASHFUNCTION_FILENAME = "/tmp/prefixGetRangeHashFunction.txt";

    /** There are several range and the prefix lies in the first range. */
    @Test
    public void firstBucket() {
        byte[][] rangeValues = new byte[][] { { 0, 1, 0, 0 }, { 0, 5, 0, 0 }, { 0, 6, 0, 0 }, { 0, 7, 0, 0 } };
        String[] generatedFilenames = generateFilenames(rangeValues.length);

        RangeHashFunction hashFunction = new RangeHashFunction(rangeValues, generatedFilenames, HASHFUNCTION_FILENAME);

        byte[] prefix = { 0, 0, 2 };
        int[] bucketIds = hashFunction.getBucketIdsFor(prefix);
        Assert.assertEquals(1, bucketIds.length);
        Assert.assertEquals(0, bucketIds[0]);
        Assert.assertEquals("bucket0.db", hashFunction.getFilename(bucketIds[0]));

    }

    /**
     * Some maximal ranges are added to the hash function and only one range of these are returned.
     */
    @Test
    public void oneBucket() {
        byte[][] rangeValues = new byte[][] { { 0, 0, 1, 0 }, { 0, 0, 5, 0 }, { 0, 0, 6, 0 }, { 0, 0, 7, 0 } };
        String[] generatedFilenames = generateFilenames(rangeValues.length);

        RangeHashFunction hashFunction = new RangeHashFunction(rangeValues, generatedFilenames, HASHFUNCTION_FILENAME);

        byte[] prefix = { 0, 0, 2 };
        int[] bucketIds = hashFunction.getBucketIdsFor(prefix);
        Assert.assertEquals(1, bucketIds.length);
        Assert.assertEquals(1, bucketIds[0]);
        Assert.assertEquals("bucket1.db", hashFunction.getFilename(bucketIds[0]));
    }

    /**
     * We search for all bucket ids that confirm to the given prefix. In this test one bucket id will be returned.
     */
    @Test
    public void twoBuckets() {
        byte[][] rangeValues = new byte[][] { { 0, 0, 1, 0 }, { 0, 0, 2, 0 }, { 0, 0, 3, 0 }, { 0, 0, 4, 0 } };
        String[] generatedFilenames = generateFilenames(rangeValues.length);

        RangeHashFunction hashFunction = new RangeHashFunction(rangeValues, generatedFilenames, HASHFUNCTION_FILENAME);

        byte[] prefix = { 0, 0, 1 };
        int[] bucketIds = hashFunction.getBucketIdsFor(prefix);
        Assert.assertEquals(2, bucketIds.length);
        Assert.assertEquals(0, bucketIds[0]);
        Assert.assertEquals(1, bucketIds[1]);
        Assert.assertEquals("bucket0.db", hashFunction.getFilename(bucketIds[0]));
        Assert.assertEquals("bucket1.db", hashFunction.getFilename(bucketIds[1]));
    }

    /**
     * We search for the bucket ids that confirm to the given prefix. In this test three buckets ids will be returned.
     */
    @Test
    public void twoBucketsInTheMiddle() {
        byte[][] rangeValues = new byte[][] { { 0, 0, 1, 0 }, { 0, 0, 1, 5 }, { 0, 0, 2, 1 }, { 0, 0, 2, 2 },
                { 0, 0, 3, 0 }, { 0, 0, 4, 0 } };
        String[] generatedFilenames = generateFilenames(rangeValues.length);

        RangeHashFunction hashFunction = new RangeHashFunction(rangeValues, generatedFilenames,HASHFUNCTION_FILENAME);

        byte[] prefix = { 0, 0, 2 };
        int[] bucketIds = hashFunction.getBucketIdsFor(prefix);
        Assert.assertEquals(3, bucketIds.length);
        Assert.assertEquals(2, bucketIds[0]);
        Assert.assertEquals(3, bucketIds[1]);
        Assert.assertEquals(4, bucketIds[2]);

        Assert.assertEquals("bucket2.db", hashFunction.getFilename(bucketIds[0]));
        Assert.assertEquals("bucket3.db", hashFunction.getFilename(bucketIds[1]));
        Assert.assertEquals("bucket4.db", hashFunction.getFilename(bucketIds[2]));
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

        RangeHashFunction hashFunction = new RangeHashFunction(rangeValues, generatedFilenames, HASHFUNCTION_FILENAME);

        byte[] prefix = { 0, 0, 3 };
        int[] bucketIds = hashFunction.getBucketIdsFor(prefix);
        Assert.assertEquals(2, bucketIds.length);
        Assert.assertEquals(4, bucketIds[0]);
        Assert.assertEquals(5, bucketIds[1]);
        Assert.assertEquals("bucket4.db", hashFunction.getFilename(bucketIds[0]));
        Assert.assertEquals("bucket5.db", hashFunction.getFilename(bucketIds[1]));
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
        RangeHashFunction hashFunction = new RangeHashFunction(rangeValues, generatedFilenames, HASHFUNCTION_FILENAME);

        byte[] prefix = { 0, 0, 4 };
        int[] bucketIds = hashFunction.getBucketIdsFor(prefix);
        Assert.assertEquals(2, bucketIds.length);
        Assert.assertEquals(5, bucketIds[0]);
        Assert.assertEquals(0, bucketIds[1]);
        Assert.assertEquals("bucket5.db", hashFunction.getFilename(bucketIds[0]));
        Assert.assertEquals("bucket0.db", hashFunction.getFilename(bucketIds[1]));
    }

    @Test
    public void inLastRange() {
        byte[][] rangeValues = new byte[][] { { 0, 0, 1, 0 }, { 0, 0, 1, 5 }, { 0, 0, 2, 1 }, { 0, 0, 2, 2 },
                { 0, 0, 3, 0 }, { 0, 0, 4, 0 } };
        String[] generatedFilenames = generateFilenames(rangeValues.length);
        RangeHashFunction hashFunction = new RangeHashFunction(rangeValues, generatedFilenames, HASHFUNCTION_FILENAME);

        byte[] prefix = { 0, 0, 5, 0 };
        int[] bucketIds = hashFunction.getBucketIdsFor(prefix);
        Assert.assertEquals(1, bucketIds.length);
        Assert.assertEquals(0, bucketIds[0]);
        Assert.assertEquals("bucket0.db", hashFunction.getFilename(bucketIds[0]));
    }

    @Test
    public void oneOverallRange() {
        byte[][] rangeValues = new byte[][] { { (byte) -1, (byte) -1, (byte) -1, (byte) -1 } };

        String[] generatedFilenames = generateFilenames(rangeValues.length);
        RangeHashFunction hashFunction = new RangeHashFunction(rangeValues, generatedFilenames, HASHFUNCTION_FILENAME);

        byte[] prefix = { -84, -60, -28 };
        int[] bucketIds = hashFunction.getBucketIdsFor(prefix);
        Assert.assertEquals(1, bucketIds.length);
        Assert.assertEquals(0, bucketIds[0]);
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

        RangeHashFunction hashFunction = new RangeHashFunction(rangeValues, generatedFilenames, HASHFUNCTION_FILENAME);

        byte[] prefix = { 0, 0, 2, 1, 1 };
        hashFunction.getBucketIdsFor(prefix);
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
}