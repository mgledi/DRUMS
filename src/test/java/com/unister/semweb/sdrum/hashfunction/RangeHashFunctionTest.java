package com.unister.semweb.sdrum.hashfunction;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import junit.framework.Assert;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.unister.semweb.sdrum.bucket.hashfunction.RangeHashFunction;
import com.unister.semweb.sdrum.utils.KeyUtils;

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
        System.out.println("############ correctHashDistribution()");
        long[] ranges = new long[] { 5, 10, 20, 30, 40 };
        byte[][] bRanges = KeyUtils.transformToByteArray(ranges);
        String[] filenames = new String[] { "0", "1", "2", "3", "1" };

        RangeHashFunction hashFunction = new RangeHashFunction(bRanges, filenames, null);

        int currentBucketId = -1;
        currentBucketId = hashFunction.getBucketId(11);
        Assert.assertEquals(2, currentBucketId);

        currentBucketId = hashFunction.getBucketId(20);
        Assert.assertEquals(2, currentBucketId);

        currentBucketId = hashFunction.getBucketId(21);
        Assert.assertEquals(3, currentBucketId);

        currentBucketId = hashFunction.getBucketId(29);
        Assert.assertEquals(3, currentBucketId);

        currentBucketId = hashFunction.getBucketId(30);
        Assert.assertEquals(3, currentBucketId);

        currentBucketId = hashFunction.getBucketId(100);
        Assert.assertEquals(0, currentBucketId);

        currentBucketId = hashFunction.getBucketId(4);
        Assert.assertEquals(0, currentBucketId);

        currentBucketId = hashFunction.getBucketId(Long.MAX_VALUE);
        Assert.assertEquals(0, currentBucketId);

        currentBucketId = hashFunction.getBucketId(Long.MIN_VALUE);
        Assert.assertEquals(0, currentBucketId);
    }

    @Test
    public void massiveTest() {
        System.out.println("############ massiveTest()");
        int numberOfRanges = 10000;
        int numberOfKeysToSearchFor = 1000000;

        long[] ranges = generateUniqueRanges(numberOfRanges);
        byte[][] bRanges = KeyUtils.transformToByteArray(ranges);
        String[] filenames = generateUniqueFilenames(numberOfRanges);

        long overallTime = 0;
        RangeHashFunction hashFunction = new RangeHashFunction(bRanges, filenames, null);
        for (int i = 0; i < numberOfKeysToSearchFor; i++) {
            long randomKey = randomGenerator.nextLong();
            if (randomKey < 0) {
                randomKey *= -1;
            }

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
        byte[][] bRanges = KeyUtils.transformToByteArray(rangeValues);
        String[] filenames = new String[] { "f1", "f2", "f3", "f4", "f5" };

        RangeHashFunction hashFunction = new RangeHashFunction(bRanges, filenames, null);
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
        byte[][] bRanges = KeyUtils.transformToByteArray(rangeValues);
        String[] filenames = new String[] { "f1", "f2", "f3", "f4", "f5" };

        RangeHashFunction hashFunction = new RangeHashFunction(bRanges, filenames, null);
        hashFunction.getFilename(6);
    }

    // TODO m.gledi should generate the corresponding hash function file.
    @Ignore
    @Test
    public void loadHashFunctionFromFile() throws IOException {
        File f = new File("src/test/resources/rangehash.hash");
        RangeHashFunction rhf = new RangeHashFunction(f);
        long l = 16L << 56;
        Assert.assertEquals(0, rhf.getBucketId(l));
        l++;
        Assert.assertEquals(1, rhf.getBucketId(l));
        Assert.assertEquals(7, rhf.getBucketId(Long.MAX_VALUE));
    }

    /* ******************************************** Data generator methods ******************** */
    /* **************************************************************************************** */

    /** Generates the specified number of positive ranges. The ranges are unique. */
    private long[] generateUniqueRanges(int numberOfRanges) {
        long[] result = new long[numberOfRanges];
        for (int i = 0; i < numberOfRanges; i++) {
            long l = randomGenerator.nextLong();
            if (l < 0) {
                l *= -1;
            }
            result[i] = l;
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