package com.unister.semweb.sdrum.syncronizer;

import java.io.File;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.unister.semweb.sdrum.TestUtils;
import com.unister.semweb.sdrum.bucket.SortMachine;
import com.unister.semweb.sdrum.storable.DummyKVStorable;
import com.unister.semweb.sdrum.synchronizer.UpdateOnlySynchronizer;
import com.unister.semweb.sdrum.utils.KeyUtils;

/**
 * Testing the UpdateOnlySynchronizer
 * 
 * @author m.gleditzsch, m.unglaub
 */
public class UpdateOnlySynchronizerTest {
    @Test
    /** This function tests, if update will be stored correctly */
    public void updateTest() throws Exception {
        TestUtils.init();
        System.out.println("============== updateTest() ==================");
        // ################ creating file with initial data
        String dbFileName = TestUtils.gp.databaseDirectory + "/test.db";
        new File(dbFileName).delete();
        DummyKVStorable[] linkDataList = new DummyKVStorable[20000];
        for (int i = 0; i < linkDataList.length; i++) {
            linkDataList[i] = DummyKVStorable.getInstance();
            linkDataList[i].setKey(KeyUtils.transformFromLong(i * 1 + 1, linkDataList[i].key.length));
        }
        TestUtils.createFile(dbFileName, linkDataList);
        Assert.assertTrue(TestUtils.checkContentFile(dbFileName, linkDataList));

        // ################ prepare Update-Array, using only pointers from insert-array
        //        DummyKVStorable[] toUpdate = new DummyKVStorable[linkDataList.length];
        //        for (int i = 0; i < linkDataList.length; i++) {
        //            toUpdate[i] = linkDataList[i];
        //        }

        DummyKVStorable[] toUpdate = new DummyKVStorable[4];
        toUpdate[3] = linkDataList[1900];
        toUpdate[3].setValue("parentCount", 2300000);
        toUpdate[1] = linkDataList[100];
        toUpdate[1].setValue("parentCount", 1111);
        toUpdate[0] = linkDataList[9000];
        toUpdate[0].setValue("parentCount", 989845);
        toUpdate[2] = linkDataList[11190];
        toUpdate[2].setValue("parentCount", 234567);

        SortMachine.quickSort(toUpdate);

        // ############### perform the update, this is the real test
        UpdateOnlySynchronizer<DummyKVStorable> updateSync = new UpdateOnlySynchronizer<DummyKVStorable>(dbFileName,
                TestUtils.gp);
        updateSync.upsert(toUpdate);

        List<DummyKVStorable> listResult = TestUtils.readFrom(dbFileName, 50000);
        DummyKVStorable[] result = listResult.toArray(new DummyKVStorable[listResult.size()]);

        // ############### check if the file was written correctly, the file have to be compared to the linkDataList
        Assert.assertArrayEquals(linkDataList, result);
    }
}
