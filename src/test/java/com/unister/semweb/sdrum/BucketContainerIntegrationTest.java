package com.unister.semweb.sdrum;

import java.io.File;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;

import com.unister.semweb.sdrum.api.SDRUM;
import com.unister.semweb.sdrum.api.SDRUM_API;
import com.unister.semweb.sdrum.bucket.BucketContainer;
import com.unister.semweb.sdrum.bucket.hashfunction.RangeHashFunction;
import com.unister.semweb.sdrum.storable.DummyKVStorable;
import com.unister.semweb.sdrum.sync.SyncManager;
import com.unister.semweb.sdrum.synchronizer.Synchronizer;
import com.unister.semweb.sdrum.utils.RangeHashFunctionTestUtils;

/**
 * It is an integration test for the {@link BucketContainer}, {@link SyncManager} and
 * the {@link Synchronizer}. Objects from these three classes are created and linked
 * together.
 * 
 * @author n.thieme
 * 
 */
public class BucketContainerIntegrationTest {
    /** The overall number of elements that will be add to the file storage. */
    private static final long numberOfElementsToAdd = (long) 1e3;

    public static final long differentElements = numberOfElementsToAdd;
    /**
     * The temporary directory that is used for the test. All files
     * that are created while testing are stored here.
     */
    private static final String TEMP_DIRECTORY = "/data/mgledi/data/newTableTest";
//    private static final File TEMP_DIRECTORY = new File("/data/mgledi/data");

    /** The maximum bucket size to be used. */
    private static int allowedElementsPerBucket = 1000;

    /** Size of the intercache that the {@link BucketContainer} is used. */
    private static int sizeOfWaitingQueue = 10;

    /** The number of buffer threads that synchronizes the data with the HDD. */
    private static int numberOfSynchronizingThreads = 1;

    /** The overall number of buckets to be used. */
    private static int numberOfBuckets = 2;


    /**
     * Before a test can start we must make sure that all file storages files are deleted
     * from the temporary directory.
     * 
     * @throws Exception
     */
    public static void initialise() throws Exception {
        Iterator<File> files = FileUtils.iterateFiles(new File(TEMP_DIRECTORY), new String[] { "db" }, false);
        while (files.hasNext()) {
            files.next().delete();
        }
    }

    /**
     * Writes test data to the file storage and measures the time. It is an integration test because
     * we can notice errors if any happen.
     * 
     * @throws Exception
     */
    public static void main(String args[]) throws Exception {
        RangeHashFunction hashFunction = RangeHashFunctionTestUtils.createTestFunction(10, 10000, 1000, TEMP_DIRECTORY + "tmp.txt", 26);
        SDRUM sdrum = SDRUM_API.createOrOpenTable(TEMP_DIRECTORY, 1000, 10, 1, hashFunction, new DummyKVStorable());
        DummyKVStorable[] data = TestUtils.generateTestdata(100000, 1);
        sdrum.insertOrMerge(data);
        sdrum.close();
    }
}
