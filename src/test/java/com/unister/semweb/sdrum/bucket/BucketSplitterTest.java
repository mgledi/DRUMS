package com.unister.semweb.sdrum.bucket;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import com.unister.semweb.sdrum.TestUtils;
import com.unister.semweb.sdrum.api.SDRUM;
import com.unister.semweb.sdrum.bucket.hashfunction.RangeHashFunction;
import com.unister.semweb.sdrum.file.FileLockException;
import com.unister.semweb.sdrum.storable.DummyKVStorable;

/**
 * This class is for Testing the BucketSplitter
 * 
 * @author m.gleditzsch
 */
public class BucketSplitterTest {
    RangeHashFunction hashFunction;
    private static final String databaseDirectory = "/tmp/createTable/db";
    private static final String configurationFile = databaseDirectory + "/" + "database.properties";
    private DummyKVStorable prototype;

    private SDRUM<DummyKVStorable> table;
    
    @Before
    public void initialise() throws Exception {
        long[] ranges = new long[] { 0, 10, 100, 1000 };
        String[] filenames = new String[] { "1.db", "2", "3.db", "4.db" };
        int[] sizes = { 1000, 1000, 1000, 1000 };
        FileUtils.deleteQuietly(new File(databaseDirectory));

        this.hashFunction = new RangeHashFunction(ranges, filenames, sizes, new File("/tmp/hash.hs"));
        this.prototype = new DummyKVStorable();

        // fill with data
        table = SDRUM.createTable( databaseDirectory, 1000,
                10000, 1, hashFunction, prototype);
        table.insertOrMerge(TestUtils.createDummyData(-100, 1500));
        table.close();
    }

    @Test
    public void generateFileName() throws IOException, FileLockException {
        BucketSplitter<DummyKVStorable> bs = new BucketSplitter<DummyKVStorable>(databaseDirectory, this.hashFunction, 3, 3);
        Assert.assertEquals(bs.generateFileName(1, hashFunction.getFilename(3)), "4_1.db");
        Assert.assertEquals(bs.generateFileName(1, hashFunction.getFilename(1)), "2_1");
    }
    
    @Test
    public void storeHashFunction() {
        fail("not written yet");
    }

    @Test
    public void moveElements() {
        fail("not written yet");
    }
    
    @Test
    public void deleteOldFile() {
        fail("not written yet");
    }
}
