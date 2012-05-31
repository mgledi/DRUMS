package com.unister.semweb.sdrum.api;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.ArrayList;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import com.unister.semweb.sdrum.TestUtils;
import com.unister.semweb.sdrum.bucket.SortMachine;
import com.unister.semweb.sdrum.bucket.hashfunction.RangeHashFunction;
import com.unister.semweb.sdrum.storable.DummyKVStorable;
import com.unister.semweb.sdrum.utils.KeyUtils;

/**
 * This class tests the {@link SDrumIterator}.
 * 
 * @author m.gleditzsch
 */
public class SDrumIteratorTest {
    RangeHashFunction hashFunction;
    private static final String databaseDirectory = "/tmp/createTable/db";

    private SDRUM<DummyKVStorable> table;
    private DummyKVStorable[] generatedData;

    @Before
    public void initialise() throws Exception {
        long[] ranges = new long[] { 0, 10, 20, 30 };
        byte[][] bRanges = KeyUtils.transformToByteArray(ranges);
        String[] filenames = new String[] { "1.db", "2", "3.db", "4.db" };
        FileUtils.deleteQuietly(new File(databaseDirectory));

        this.hashFunction = new RangeHashFunction(bRanges, filenames, new File("/tmp/hash.hs"));

        // fill with data
        table = SDRUM_API.createTable(databaseDirectory, hashFunction, TestUtils.gp);
        // remember, the element with key 0 is ignored
        generatedData = TestUtils.createDummyData(1, 50);
        SortMachine.quickSort(generatedData);
        table.insertOrMerge(generatedData);
        table.close();
    }

    @Test
    public void iteratorTest() throws FileStorageException {
        SDrumIterator<DummyKVStorable> it = new SDrumIterator<DummyKVStorable>("/tmp/createTable/db", hashFunction,
                DummyKVStorable.getInstance());
        ArrayList<DummyKVStorable> elements = new ArrayList<DummyKVStorable>();
        while (it.hasNext()) {
            Object d = it.next();
            elements.add((DummyKVStorable) d);
            // elements should be read in ascending order
        }
        assertEquals(generatedData.length, elements.size());
    }
}
