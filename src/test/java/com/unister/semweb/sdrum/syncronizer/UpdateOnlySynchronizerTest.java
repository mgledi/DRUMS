package com.unister.semweb.sdrum.syncronizer;

import java.io.File;

import junit.framework.Assert;

import org.junit.Test;

import com.unister.semweb.sdrum.TestUtils;
import com.unister.semweb.sdrum.bucket.SortMachine;
import com.unister.semweb.sdrum.storable.DummyKVStorable;
import com.unister.semweb.sdrum.synchronizer.UpdateOnlySynchronizer;

/**
 * Testing the UpdateOnlySynchronizer
 * 
 * @author m.gleditzsch, m.unglaub
 */
public class UpdateOnlySynchronizerTest {

    @Test
    /** This function tests, if update will be stored correctly */
    public void updateTest() throws Exception {
        System.out.println("============== updateTest()" );
        // ################ creating file with initial data
        String dbFileName = "/tmp/test.db";
        new File(dbFileName).delete();
        DummyKVStorable[] linkDataList = new DummyKVStorable[2000];
        for (int i = 0; i < linkDataList.length; i++) {
            linkDataList[i] = new DummyKVStorable();
            linkDataList[i].setKey(i*1 + 1);
            linkDataList[i].setTimestamp(0);
        }
        TestUtils.createFile(dbFileName, linkDataList);
        Assert.assertTrue(TestUtils.checkContentFile(dbFileName, linkDataList));

        // ################ prepare Update-Array, using only pointers from insert-array
        DummyKVStorable[] toUpdate = new DummyKVStorable[3];
        toUpdate[2] = linkDataList[1900];
        toUpdate[2].setTimestamp(2300000);
        toUpdate[1] = linkDataList[100];
        toUpdate[1].setTimestamp(2300000);
        toUpdate[0] = linkDataList[90];
        toUpdate[0].setTimestamp(2300000);
        
        SortMachine.quickSort(toUpdate);
        
        // ############### perform the update, this is the real test
        UpdateOnlySynchronizer<DummyKVStorable> updateSync = new UpdateOnlySynchronizer<DummyKVStorable>(dbFileName, new DummyKVStorable());
        updateSync.upsert(toUpdate);

        // ############### check if the file was written correctly, the file have to be compared to the linkDataList
        Assert.assertTrue(TestUtils.checkContentFile(dbFileName, linkDataList));
    }
}
