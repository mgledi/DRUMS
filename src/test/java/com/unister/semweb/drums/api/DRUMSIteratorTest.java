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
package com.unister.semweb.drums.api;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.ArrayList;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import com.unister.semweb.drums.TestUtils;
import com.unister.semweb.drums.api.DRUMSException;
import com.unister.semweb.drums.api.DRUMS;
import com.unister.semweb.drums.api.DRUMSInitialisation;
import com.unister.semweb.drums.api.DRUMSIterator;
import com.unister.semweb.drums.bucket.SortMachine;
import com.unister.semweb.drums.bucket.hashfunction.RangeHashFunction;
import com.unister.semweb.drums.storable.DummyKVStorable;
import com.unister.semweb.drums.utils.KeyUtils;

/**
 * This class tests the {@link DRUMSIterator}.
 * 
 * @author Martin Nettling
 */
public class DRUMSIteratorTest {
    RangeHashFunction hashFunction;

    private DRUMS<DummyKVStorable> table;
    private DummyKVStorable[] generatedData;

    @Before
    public void initialise() throws Exception {
        new File(TestUtils.gp.databaseDirectory).mkdirs();
        long[] ranges = new long[] { 0, 10, 20, 30 };
        byte[][] bRanges = KeyUtils.transformToByteArray(ranges);
        String[] filenames = new String[] { "1.db", "2", "3.db", "4.db" };
        FileUtils.deleteQuietly(new File(TestUtils.gp.databaseDirectory));
        this.hashFunction = new RangeHashFunction(bRanges, filenames, "/tmp/hash.hs");

        // fill with data
        table = DRUMSInitialisation.createTable(hashFunction, TestUtils.gp);
        // remember, the element with key 0 is ignored
        generatedData = TestUtils.createDummyData(1, 50);
        SortMachine.quickSort(generatedData);
        table.insertOrMerge(generatedData);
        table.close();
    }

    @Test
    public void iteratorTest() throws DRUMSException, InterruptedException {
        table = DRUMSInitialisation.openTable(hashFunction, DRUMS.AccessMode.READ_ONLY, TestUtils.gp);
        DRUMSIterator<DummyKVStorable> it = table.getIterator();
        ArrayList<DummyKVStorable> elements = new ArrayList<DummyKVStorable>();
        while (it.hasNext()) {
            Object d = it.next();
            elements.add((DummyKVStorable) d);
            // elements should be read in ascending order
        }
        assertEquals(generatedData.length, elements.size());
        table.close();
    }
}
