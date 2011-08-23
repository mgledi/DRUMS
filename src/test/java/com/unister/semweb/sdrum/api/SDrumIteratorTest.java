package com.unister.semweb.sdrum.api;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import com.unister.semweb.sdrum.TestUtils;
import com.unister.semweb.sdrum.bucket.hashfunction.RangeHashFunction;
import com.unister.semweb.sdrum.storable.DummyKVStorable;


public class SDrumIteratorTest {
    RangeHashFunction hashFunction;
    private static final String databaseDirectory = "/tmp/createTable/db";
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
        table = SDRUM_API.createTable( databaseDirectory, 1000,
                10000, 1, hashFunction, prototype);
        table.insertOrMerge(TestUtils.createDummyData(-100, -5));
        table.insertOrMerge(TestUtils.createDummyData(5, 200));
        table.close();
    }
    @Test
    public void iteratoreTest() throws FileStorageException {
        SDrumIterator<DummyKVStorable> it = new SDrumIterator<DummyKVStorable>("/tmp/createTable/db", hashFunction, new DummyKVStorable());
        while(it.hasNext()) {
            it.next();
        }
    }
}
