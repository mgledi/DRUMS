package com.unister.semweb.sdrum;

import java.io.File;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;


import com.unister.semweb.sdrum.bucket.Bucket;
import com.unister.semweb.sdrum.bucket.BucketContainer;
import com.unister.semweb.sdrum.bucket.hashfunction.FirstBitHashFunction;
import com.unister.semweb.sdrum.storable.DummyKVStorable;
import com.unister.semweb.sdrum.sync.SyncManager;
import com.unister.semweb.sdrum.synchronizer.Synchronizer;
import com.unister.semweb.sdrum.synchronizer.SynchronizerFactory;

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
    private static final long numberOfElementsToAdd = (long) 1e6;

    public static final long differentElements = numberOfElementsToAdd;
    /**
     * The temporary directory that is used for the test. All files
     * that are created while testing are stored here.
     */
    private static final String TEMP_DIRECTORY = "/data/tmp/filestorage/db/";
//    private static final File TEMP_DIRECTORY = new File("/data/mgledi/data");

    /** The maximum bucket size to be used. */
    private static int allowedElementsPerBucket = 40000;

    /** Size of the intercache that the {@link BucketContainer} is used. */
    private static int sizeOfWaitingQueue = 100000;

    /** The number of buffer threads that synchronizes the data with the HDD. */
    private static int numberOfSynchronizingThreads = 1;

    /** The overall number of buckets to be used. */
    private static int numberOfBuckets = 16;


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
    // @Test
    public static void main(String args[]) throws Exception {
        initialise();
        long startTime = System.currentTimeMillis();
        FirstBitHashFunction hashFunction = new FirstBitHashFunction(numberOfBuckets);
        Bucket<DummyKVStorable>[] buckets = new Bucket[hashFunction.getNumberOfBuckets()];
        for (int i = 0; i < buckets.length; i++) {
            buckets[i] = new Bucket<DummyKVStorable>(i, allowedElementsPerBucket, new DummyKVStorable());
        }

        BucketContainer<DummyKVStorable> bucketContainer = new BucketContainer<DummyKVStorable>(buckets, sizeOfWaitingQueue, hashFunction);
        SyncManager<DummyKVStorable> buffer = new SyncManager<DummyKVStorable>(bucketContainer, numberOfSynchronizingThreads, TEMP_DIRECTORY, new SynchronizerFactory<DummyKVStorable>(new DummyKVStorable()));
        buffer.start();

        StringBuilder sb = new StringBuilder();
        long writeTimeStart = System.currentTimeMillis();
        for (long i = 0; i < numberOfElementsToAdd; i ++) {
            if(i % 1e5 == 0) {
                writeTimeStart = System.currentTimeMillis();
                System.out.println("Writing next " + 1e5 + " elements " + i + " / "
                        + numberOfElementsToAdd + "\t");
                sb.append("Writing next " + 1e5 + " elements " + i + " / "
                        + numberOfElementsToAdd + "\t");
            }
            DummyKVStorable[] testdata = TestUtils.generateTestdata(1, Long.MAX_VALUE,
                    (long) differentElements);
//            testdata[0].setKey(i);
            bucketContainer.addToCache(testdata);
            if(i % 1e5 == 0) {
                sb.append((System.currentTimeMillis() - writeTimeStart) + "ms needed\n");
            }
        }
        System.out.println(sb);
        buffer.shutdown();
        buffer.join();
        System.out.println("Time needed: " + (System.currentTimeMillis() - startTime));

    }

}
