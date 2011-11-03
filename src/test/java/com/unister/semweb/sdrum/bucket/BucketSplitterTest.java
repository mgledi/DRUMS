package com.unister.semweb.sdrum.bucket;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import com.unister.semweb.sdrum.TestUtils;
import com.unister.semweb.sdrum.api.SDRUM;
import com.unister.semweb.sdrum.api.SDRUM_API;
import com.unister.semweb.sdrum.bucket.hashfunction.RangeHashFunction;
import com.unister.semweb.sdrum.file.FileLockException;
import com.unister.semweb.sdrum.storable.DummyKVStorable;
import com.unister.semweb.sdrum.utils.KeyUtils;

/**
 * This class is for Testing the BucketSplitter
 * 
 * @author m.gleditzsch
 */
public class BucketSplitterTest {
    RangeHashFunction hashFunction;
    private static final String databaseDirectory = "/tmp/createTable/db";

    private static final String hashFunctionFilename = "/tmp/hash.hs";

    private DummyKVStorable prototype;

    private SDRUM<DummyKVStorable> table;
    private DummyKVStorable[] testData;

    @Before
    public void initialise() throws Exception {
        long[] ranges = new long[] { 1, 10, 100, 1000 };
        byte[][] bRanges = KeyUtils.transformToByteArray(ranges);
        String[] filenames = new String[] { "1.db", "2", "3.db", "4.db" };
        int[] sizes = { 1000, 1000, 1000, 1000 };
        FileUtils.deleteQuietly(new File(databaseDirectory));

        this.hashFunction = new RangeHashFunction(bRanges, filenames, sizes, new File(hashFunctionFilename));
        this.prototype = new DummyKVStorable();

        // fill with data
        startSDRUM(hashFunction);
        testData = TestUtils.createDummyData(-100, 1500);
        table.insertOrMerge(testData);
        stopSDRUM();
    }

    private void startSDRUM(RangeHashFunction hashFunction) throws Exception {
        table = SDRUM_API.createOrOpenTable(databaseDirectory, 1000,
                10000, 1, hashFunction, prototype);
        table.setHashFunction(hashFunction);
    }

    private void stopSDRUM() throws Exception {
        table.close();
    }

    @Test
    public void generateFileName() throws IOException, FileLockException {
        BucketSplitter<DummyKVStorable> bs = new BucketSplitter<DummyKVStorable>(databaseDirectory, this.hashFunction,
                3, 3);
        Assert.assertEquals(bs.generateFileName(1, hashFunction.getFilename(3)), "4_1.db");
        Assert.assertEquals(bs.generateFileName(1, hashFunction.getFilename(1)), "2_1");
    }

    @Test
    public void storeHashFunction() {
        fail("not written yet");
    }

    @Test
    public void moveElements() throws Exception {
        new BucketSplitter<DummyKVStorable>(databaseDirectory, this.hashFunction, 2, 4);

        RangeHashFunction reReadFunction = new RangeHashFunction(new File(hashFunctionFilename));
        startSDRUM(reReadFunction);

        List<DummyKVStorable> storedElementNewBucket1 = table.read(2, 0, 10000);
        List<DummyKVStorable> storedElementNewBucket2 = table.read(3, 0, 10000);
        List<DummyKVStorable> storedElementNewBucket3 = table.read(4, 0, 10000);
        List<DummyKVStorable> storedElementNewBucket4 = table.read(5, 0, 10000);
        stopSDRUM();

        Set<DummyKVStorable> mergedData = merge(storedElementNewBucket1, storedElementNewBucket2,
                storedElementNewBucket3, storedElementNewBucket4);

        DummyKVStorable[] distributedData = TestUtils.createDummyData(10, 100);
        boolean areEqual = areEqual(distributedData, mergedData);
        Assert.assertTrue(areEqual);
    }

    private Set<DummyKVStorable> merge(List<DummyKVStorable>... toMerge) {
        Set<DummyKVStorable> result = new HashSet<DummyKVStorable>();
        for (List<DummyKVStorable> toAdd : toMerge) {
            result.addAll(toAdd);
        }
        return result;
    }

    private boolean areEqual(DummyKVStorable[] first, Set<DummyKVStorable> second) {
        List<DummyKVStorable> converted = Arrays.asList(first);
        Set<DummyKVStorable> convertedSet = new HashSet<DummyKVStorable>(converted);
        if (second.equals(convertedSet)) {
            return true;
        } else {
            return false;
        }
    }

    @Test
    public void deleteOldFile() {
        fail("not written yet");
    }
}
