package com.unister.semweb.superfast.storage.syncronizer;

import java.io.File;

import junit.framework.Assert;

import org.junit.Test;

import com.unister.semweb.superfast.storage.TestUtils;
import com.unister.semweb.superfast.storage.bucket.SortMachine;
import com.unister.semweb.superfast.storage.storable.DummyKVStorable;
import com.unister.semweb.superfast.storage.synchronizer.UpdateOnlySynchronizer;

/**
 * Testing the UpdateOnlySynchronizer
 * 
 * @author m.gleditzsch, m.unglaub
 */
public class UpdateOnlySynchronizerTest {

    @Test
    /** This function tests, if update will be stored correctly */
    public void updateTest() throws Exception {
        // ################ creating file with initial data
        String dbFileName = "/tmp/test.db";
        new File(dbFileName).delete();
        DummyKVStorable[] linkDataList = new DummyKVStorable[2000];
        for (int i = 0; i < linkDataList.length; i++) {
            linkDataList[i] = new DummyKVStorable();
            linkDataList[i].setKey(i*10 + 1);
            linkDataList[i].setTimestamp(0);
        }
        TestUtils.createFile(dbFileName, linkDataList);
        Assert.assertTrue(TestUtils.checkContentFile(dbFileName, linkDataList));

        // ################ prepare Update-Array, using only pointers from insert-array
        DummyKVStorable[] toUpdate = new DummyKVStorable[3];
        toUpdate[2] = linkDataList[1900];
        toUpdate[2].setTimestamp(230);
        toUpdate[1] = linkDataList[100];
        toUpdate[1].setTimestamp(230);
        toUpdate[0] = linkDataList[90];
        toUpdate[0].setTimestamp(230);
        SortMachine.quickSort(toUpdate);
        
        // ############### perform the update, this is the real test
        UpdateOnlySynchronizer<DummyKVStorable> updateSync = new UpdateOnlySynchronizer<DummyKVStorable>(dbFileName, new DummyKVStorable());
        updateSync.upsert(toUpdate);

        // ############### check if the file was written correctly, the file have to be compared to the linkDataList
        Assert.assertTrue(TestUtils.checkContentFile(dbFileName, linkDataList));
    }
}
