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
package com.unister.semweb.drums.syncronizer;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.unister.semweb.drums.TestUtils;
import com.unister.semweb.drums.storable.DummyKVStorable;
import com.unister.semweb.drums.synchronizer.UpdateOnlySynchronizer;
import com.unister.semweb.drums.utils.AbstractKVStorableComparator;
import com.unister.semweb.drums.utils.Bytes;
import com.unister.semweb.drums.utils.KeyUtils;

/**
 * Testing the UpdateOnlySynchronizer
 * 
 * @author Martin Nettling, m.unglaub
 */
public class UpdateOnlySynchronizerTest {

    @Test
    /** This function tests, if update will be stored correctly */
    public void updateTest() throws Exception {
        System.out.println("============== updateTest() ==================");
        // ################ creating file with initial data
        String dbFileName = "/tmp/test.db";
        new File(dbFileName).delete();
        DummyKVStorable[] linkDataList = new DummyKVStorable[204000];
        for (int i = 0; i < linkDataList.length; i++) {
            linkDataList[i] = DummyKVStorable.getInstance();
            linkDataList[i].setKey(Bytes.toBytes(i * 1 + 1l));
        }
        TestUtils.createFile(dbFileName, linkDataList);
        Assert.assertTrue(TestUtils.checkContentFile(dbFileName, linkDataList));

        // ################ prepare Update-Array, using only pointers from insert-array
        // DummyKVStorable[] toUpdate = new DummyKVStorable[linkDataList.length];
        // for (int i = 0; i < linkDataList.length; i++) {
        // toUpdate[i] = linkDataList[i];
        // }

        DummyKVStorable[] toUpdate = new DummyKVStorable[4];
        toUpdate[3] = linkDataList[1900];
        toUpdate[3].setValue("parentCount", 2300000);
        toUpdate[1] = linkDataList[100];
        toUpdate[1].setValue("parentCount", 1111);
        toUpdate[0] = linkDataList[9000];
        toUpdate[0].setValue("parentCount", 989845);
        toUpdate[2] = linkDataList[11190];
        toUpdate[2].setValue("parentCount", 234567);

        Arrays.sort(toUpdate, new AbstractKVStorableComparator());
        // ############### perform the update, this is the real test
        UpdateOnlySynchronizer<DummyKVStorable> updateSync = new UpdateOnlySynchronizer<DummyKVStorable>(dbFileName,
                TestUtils.gp);
        updateSync.upsert(toUpdate);

        List<DummyKVStorable> listResult = TestUtils.readFrom(dbFileName, 50000);
        DummyKVStorable[] result = listResult.toArray(new DummyKVStorable[listResult.size()]);

        // ############### check if the file was written correctly, the file have to be compared to the linkDataList
        Assert.assertTrue(TestUtils.checkContentFile(dbFileName, linkDataList));
    }
}
