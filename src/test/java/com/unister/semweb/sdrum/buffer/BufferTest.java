package com.unister.semweb.sdrum.buffer;

import java.io.File;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.unister.semweb.sdrum.TestUtils;
import com.unister.semweb.sdrum.bucket.Bucket;
import com.unister.semweb.sdrum.bucket.BucketContainer;
import com.unister.semweb.sdrum.bucket.hashfunction.AbstractHashFunction;
import com.unister.semweb.sdrum.bucket.hashfunction.FirstBitHashFunction;
import com.unister.semweb.sdrum.storable.DummyKVStorable;
import com.unister.semweb.sdrum.storable.KVStorable;
import com.unister.semweb.sdrum.sync.SyncManager;
import com.unister.semweb.sdrum.synchronizer.SynchronizerFactory;

/** Here we tests whether the Buffer takes the messages from the BucketContainer. */
public class BufferTest {
    private static final Logger log = LoggerFactory.getLogger(BufferTest.class);
    private static final String TEMP_DIRECTORY_NAME = "/tmp/";
    private static final File TEMP_DIRECTORY = new File(TEMP_DIRECTORY_NAME);

    /** Deleting all database files from the temproary directory. */
    @Before
    public void initialise() throws Exception {
        Iterator<File> files = FileUtils.iterateFiles(TEMP_DIRECTORY, new String[] { "db" }, false);
        while (files.hasNext()) {
            files.next().delete();
        }
    }

    @Test
    public void bufferSimpletTest2Bucket() throws Exception {
        Bucket[] testdata = TestUtils.generateBuckets(10, 2, 10);

        AbstractHashFunction bucketComputerFunction = new FirstBitHashFunction(2);
        BucketContainer bucketContainer = new BucketContainer(testdata, 100, bucketComputerFunction);

        SyncManager<DummyKVStorable> buffer = new SyncManager<DummyKVStorable>(bucketContainer, 1, TEMP_DIRECTORY_NAME,
                new SynchronizerFactory<DummyKVStorable>(new DummyKVStorable()));
        buffer.start();
        buffer.shutdown();
        buffer.join();

        boolean bucketsEmpty = allEmpty(testdata);
        Assert.assertTrue(bucketsEmpty);

        boolean doDBFileExists = doDBFilesExistsAndHaveTheRightSize(1, 10);
        Assert.assertTrue(doDBFileExists);
    }

    @Test
    public void bufferSimpletTest8Buckets() throws Exception {
        Bucket[] testdata = TestUtils.generateBuckets(10, 8, 10);
        AbstractHashFunction bucketComputerFunction = new FirstBitHashFunction(8);
        BucketContainer bucketContainer = new BucketContainer(testdata, 100, bucketComputerFunction);

        SyncManager<DummyKVStorable> buffer = new SyncManager<DummyKVStorable>(bucketContainer, 1, TEMP_DIRECTORY_NAME,
                new SynchronizerFactory<DummyKVStorable>(new DummyKVStorable()));buffer.start();
        buffer.shutdown();
        buffer.join();

        boolean bucketsEmpty = allEmpty(testdata);
        Assert.assertTrue(bucketsEmpty);

        boolean doDBFilesExists = doDBFilesExistsAndHaveTheRightSize(8, 10);
        Assert.assertTrue(doDBFilesExists);
    }

    @Test
    public void bufferSimpletTest1Bucket2Threads() throws Exception {
        Bucket[] testdata = TestUtils.generateBuckets(1, 1, 1);
        AbstractHashFunction bucketComputerFunction = new FirstBitHashFunction(1);
        BucketContainer bucketContainer = new BucketContainer(testdata, 100, bucketComputerFunction);

        SyncManager<DummyKVStorable> buffer = new SyncManager<DummyKVStorable>(bucketContainer, 2, TEMP_DIRECTORY_NAME,
                new SynchronizerFactory<DummyKVStorable>(new DummyKVStorable()));buffer.start();
                
        buffer.shutdown();
        buffer.join();

        boolean bucketsEmpty = allEmpty(testdata);
        Assert.assertTrue(bucketsEmpty);

        boolean doDBFileExists = doDBFilesExistsAndHaveTheRightSize(1, 1);
        Assert.assertTrue(doDBFileExists);
    }

    @Test
    public void bufferSimpletTest100Bucket4Threads() throws Exception {
        Bucket[] testdata = TestUtils.generateBuckets(10000, 100, 10000);
        System.out.println("**** Number of buckets (array):  " + testdata.length);
        AbstractHashFunction bucketComputerFunction = new FirstBitHashFunction(100);
        System.out.println("************ Number of buckets********************** "
                + bucketComputerFunction.getNumberOfBuckets());
        BucketContainer bucketContainer = new BucketContainer(testdata, 100, bucketComputerFunction);

        SyncManager<DummyKVStorable> buffer = new SyncManager<DummyKVStorable>(bucketContainer, 4, TEMP_DIRECTORY_NAME,
                new SynchronizerFactory<DummyKVStorable>(new DummyKVStorable()));
        buffer.start();
        buffer.shutdown();
        buffer.join();

        boolean bucketsEmpty = allEmpty(testdata);
        Assert.assertTrue(bucketsEmpty);

        boolean doDBFileExists = doDBFilesExistsAndHaveTheRightSize(100, 10000);
        Assert.assertTrue(doDBFileExists);
    }

    @Test
    public void bufferSimpletTest1BucketLessThanThreshold() throws Exception {
        Bucket<DummyKVStorable>[] testdata = TestUtils.generateBuckets(100, 1, 10);
        AbstractHashFunction bucketComputerFunction = new FirstBitHashFunction(1);
        BucketContainer<DummyKVStorable> bucketContainer = new BucketContainer<DummyKVStorable>(testdata, 100, bucketComputerFunction);

        SyncManager<DummyKVStorable> buffer = new SyncManager<DummyKVStorable>(bucketContainer, 1, TEMP_DIRECTORY_NAME,
                new SynchronizerFactory<DummyKVStorable>(new DummyKVStorable()));
        buffer.start();
        buffer.shutdown();
        buffer.join();

        boolean bucketsEmpty = allEmpty(testdata);
        Assert.assertTrue(bucketsEmpty);

        File existingFile = new File(TEMP_DIRECTORY_NAME + "/0.db");
        Assert.assertTrue(existingFile.exists());
    }

    @Test
    public void bufferSimpletTest1BucketLessThanThreshold4Threads() throws Exception {
        Bucket<DummyKVStorable>[] testdata = TestUtils.generateBuckets(100, 1, 10);
        AbstractHashFunction bucketComputerFunction = new FirstBitHashFunction(1);
        BucketContainer<DummyKVStorable> bucketContainer = new BucketContainer<DummyKVStorable>(testdata, 100, bucketComputerFunction);

        SyncManager<DummyKVStorable> buffer = new SyncManager<DummyKVStorable>(bucketContainer, 4, TEMP_DIRECTORY_NAME,
                new SynchronizerFactory<DummyKVStorable>(new DummyKVStorable()));
        buffer.start();
        Thread.sleep(1000);
        buffer.shutdown();
        buffer.join();

        boolean bucketsEmpty = allEmpty(testdata);
        Assert.assertTrue(bucketsEmpty);

        File existingFile = new File(TEMP_DIRECTORY_NAME + "/0.db");
        Assert.assertTrue(existingFile.exists());
    }

    @Test
    public void severalBucketAddings() throws Exception {
        Bucket<DummyKVStorable>[] testdata = TestUtils.generateBuckets(100, 1, 10);
        AbstractHashFunction bucketComputerFunction = new FirstBitHashFunction(1);
        BucketContainer<DummyKVStorable> bucketContainer = new BucketContainer<DummyKVStorable>(testdata, 100, bucketComputerFunction);

        SyncManager<DummyKVStorable> buffer = new SyncManager<DummyKVStorable>(bucketContainer, 1, TEMP_DIRECTORY_NAME,
                new SynchronizerFactory<DummyKVStorable>(new DummyKVStorable()));
        buffer.start();

        DummyKVStorable[] newTestdata = TestUtils.generateTestdataDifferentFrom(
                castKVStorableToLinkData(testdata[0].getBackend()), 100);

        // wait until all elements where written
        while(testdata[0].elementsInBucket != 0 ){
            Thread.sleep(10);
        }
        testdata[0].addAll(newTestdata);
        buffer.shutdown();
        buffer.join();

        boolean allFileExists = doDBFilesExistsAndHaveTheRightSize(1, 110);
        Assert.assertTrue(allFileExists);
    }

    private DummyKVStorable[] castKVStorableToLinkData(KVStorable[] data) {
        DummyKVStorable[] newData = new DummyKVStorable[data.length];
        for (int i = 0; i < data.length; i++) {
            newData[i] = (DummyKVStorable) data[i];
        }
        return newData;
    }

    /** Examines whether all of the given buckets has no elements. */
    private boolean allEmpty(Bucket[] toExamine) {
        boolean result = true;
        for (Bucket oneBucket : toExamine) {
            if (oneBucket.size() != 0) {
                return false;
            }
        }
        return result;
    }

    private boolean doDBFilesExistsAndHaveTheRightSize(int numberOfFiles, int numberEntriesPerFile) {
        long sizePerFile = numberEntriesPerFile * 28;
        boolean result = true;
        for (int i = 0; i < numberOfFiles; i++) {
            File tempFile = new File(TEMP_DIRECTORY_NAME + "/" + i + ".db");
            if (!tempFile.exists() && tempFile.length() != sizePerFile)
                return false;
        }
        return result;
    }

    private boolean allNonEmpty(Bucket[] toExamine) {
        for (Bucket oneBucket : toExamine) {
            if (oneBucket.size() == 0) {
                return false;
            }
        }
        return true;
    }
}
