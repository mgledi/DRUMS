package com.unister.semweb.sdrum.hashfunction;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import junit.framework.Assert;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.unister.semweb.sdrum.bucket.hashfunction.RangeHashFunction;

/**
 * Tests the the distribution of the {@link RangeHashFunction}, meaning that the key is mapped to the right bucket id.
 * 
 * @author n.thieme
 * 
 */
public class RangeHashFunctionTest {
    private static final Logger log = LoggerFactory.getLogger(RangeHashFunctionTest.class);
    private static Random randomGenerator;

    @BeforeClass
    public static void initialise() {
        randomGenerator = new Random(System.currentTimeMillis());
    }

    /** Tests whether the key is mapped to the right bucket id. */
    @Test
    public void correctHashDistribution() {
        long[] ranges = new long[] { -10, 0, 10, 20, 30 };
        String[] filenames = new String[] { "0", "1", "2", "3", "1" };

        RangeHashFunction hashFunction = new RangeHashFunction(ranges, filenames);

        int currentBucketId = -1;
        currentBucketId = hashFunction.getBucketId(5);
        Assert.assertEquals(2, currentBucketId);

        currentBucketId = hashFunction.getBucketId(10);
        Assert.assertEquals(2, currentBucketId);

        currentBucketId = hashFunction.getBucketId(11);
        Assert.assertEquals(3, currentBucketId);

        currentBucketId = hashFunction.getBucketId(19);
        Assert.assertEquals(3, currentBucketId);

        currentBucketId = hashFunction.getBucketId(20);
        Assert.assertEquals(3, currentBucketId);

        currentBucketId = hashFunction.getBucketId(100);
        Assert.assertEquals(0, currentBucketId);

        currentBucketId = hashFunction.getBucketId(-10);
        Assert.assertEquals(0, currentBucketId);

        currentBucketId = hashFunction.getBucketId(25);
        Assert.assertEquals(1, currentBucketId);
        
        currentBucketId = hashFunction.getBucketId(Long.MAX_VALUE);
        Assert.assertEquals(0, currentBucketId);

        currentBucketId = hashFunction.getBucketId(Long.MIN_VALUE);
        Assert.assertEquals(0, currentBucketId);
    }

    @Test
    public void massiveTest() {
        int numberOfRanges = 100000;
        int numberOfKeysToSearchFor = 10000;

        long[] ranges = generateUniqueRanges(numberOfRanges);
        String[] filenames = generateUniqueFilenames(numberOfRanges);

        long overallTime = 0;
        RangeHashFunction hashFunction = new RangeHashFunction(ranges, filenames);
        for (int i = 0; i < numberOfKeysToSearchFor; i++) {
            long randomKey = randomGenerator.nextLong();

            long startTime = System.currentTimeMillis();
            try {
                hashFunction.getBucketId(randomKey);
            } catch (Throwable ex) {
                log.error("An error occurred: {}", ex.getMessage());
                log.error("Key to find: {}", randomKey);
                log.error("Ranges:");
                logOut(ranges);
                log.error("Exception that was thrown: {}", ex);

                break;
            }
            overallTime = overallTime + (System.currentTimeMillis() - startTime);
        }

        log.info("Time taken for {} searches in {} ranges: {}", new Object[] { numberOfKeysToSearchFor, numberOfRanges,
                overallTime });
    }

    /**
     * Tests the <code>getFileNameOf()</code> method of the {@link RangeHashFunction} if the bucket ids are valid.
     * 
     * @throws Exception
     */
    @Test
    public void testSearchForBucketIdIndex() throws Exception {
        long[] rangeValues = new long[] { 1, 2, 3, 4, 5 };
        String[] filenames = new String[] { "f1", "f2", "f3", "f4", "f5" };

        RangeHashFunction hashFunction = new RangeHashFunction(rangeValues, filenames);
        Assert.assertEquals("f1", hashFunction.getFilename(0));
        Assert.assertEquals("f2", hashFunction.getFilename(1));
        Assert.assertEquals("f3", hashFunction.getFilename(2));
        Assert.assertEquals("f4", hashFunction.getFilename(3));
        Assert.assertEquals("f5", hashFunction.getFilename(4));
    }

    /**
     * Tests the <code>getFilenameOf()</code> of the {@link RangeHashFunction} if a filename for an invalid bucket is
     * searched.
     * 
     * @throws Exception
     */
    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void testSearchForBucketIdIndexInvalid() throws Exception {
        long[] rangeValues = new long[] { 1, 2, 3, 4, 5 };
        String[] filenames = new String[] { "f1", "f2", "f3", "f4", "f5" };

        RangeHashFunction hashFunction = new RangeHashFunction(rangeValues, filenames);
        hashFunction.getFilename(6);
    }

    /* ******************************************** Data generator methods ******************** */
    /* **************************************************************************************** */

    /** Generates the specified number of ranges. The ranges are unique. */
    private long[] generateUniqueRanges(int numberOfRanges) {
        Set<Long> randomRanges = new HashSet<Long>();

        do {
            for (int i = 0; i < numberOfRanges; i++) {
                randomRanges.add(randomGenerator.nextLong());
            }
        } while (randomRanges.size() < numberOfRanges);

        long[] result = new long[numberOfRanges];
        int i = 0;
        for (Long oneRange : randomRanges) {
            result[i] = oneRange;
            i++;
            if (i >= numberOfRanges) {
                break;
            }
        }

        return result;
    }

    /**
     * Takes the bucket ids and generates from that the file names. Suppose the bucket ids are unique, the file names
     * will be unique.
     */
    private String[] generateUniqueFilenames(int numberOfFilenames) {
        String[] result = new String[numberOfFilenames];
        for (int i = 0; i < numberOfFilenames; i++) {
            result[i] = "Filename" + i;
        }

        return result;
    }

    /* ************************ Logger methods ************************* */
    /* ***************************************************************** */
    private void logOut(long[] toLog) {
        for (Object oneObject : toLog) {
            log.info(" One object: {}", oneObject);
        }
    }
}