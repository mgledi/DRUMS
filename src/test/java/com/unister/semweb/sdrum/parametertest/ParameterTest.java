package com.unister.semweb.sdrum.parametertest;

import java.io.File;
import java.text.NumberFormat;
import java.util.Iterator;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.unister.semweb.sdrum.TestUtils;
import com.unister.semweb.sdrum.bucket.Bucket;
import com.unister.semweb.sdrum.bucket.BucketContainer;
import com.unister.semweb.sdrum.bucket.BucketContainerException;
import com.unister.semweb.sdrum.bucket.hashfunction.FirstBitHashFunction;
import com.unister.semweb.sdrum.storable.DummyKVStorable;
import com.unister.semweb.sdrum.sync.SyncManager;
import com.unister.semweb.sdrum.synchronizer.SynchronizerFactory;

/**
 * Tests the performance of the file storage. It makes several tests to varying the parameters of the file storage. The
 * results are written to the log.
 * 
 * @author n.thieme
 * 
 */
public class ParameterTest {
    private static final String PERFORMANCE_TEST_PROPERTIES = "performanceTest.properties";
    private static final Logger log = LoggerFactory.getLogger(ParameterTest.class);

    private int numberOfIncrements = 1;
    private int numberOfElementsToAdd = (int) 1e7;
    private int numberOfBuckets = 10;
    private int sizeOfWaitingQueue = 10000;
    private int numberOfThreads = 2;
    private int numberOfChunksToRead = 100;
    private int allowedElementsPerBucket;
    private String directoryOfFiles;

    /**
     * Initialises the performance test configuration parameters from the configuration file.
     * 
     * @throws Exception
     */
    @Before
    public void initialise() throws Exception {
        PropertiesConfiguration propertiesConfiguration = new PropertiesConfiguration(PERFORMANCE_TEST_PROPERTIES);
        this.numberOfIncrements = propertiesConfiguration.getInt("numberOfIncrements", 1);
        this.numberOfElementsToAdd = (int) propertiesConfiguration.getDouble("numberOfElementsToAdd", 1000000);
        this.numberOfBuckets = propertiesConfiguration.getInt("numberOfBuckets", 64);
        this.sizeOfWaitingQueue = propertiesConfiguration.getInt("sizeOfWaitingQueue", 1000);
        this.numberOfThreads = propertiesConfiguration.getInt("numberOfThreads", 2);
        this.allowedElementsPerBucket = propertiesConfiguration.getInt("allowedElementsPerBucket", 1000);
        this.directoryOfFiles = propertiesConfiguration.getString("directoryOfFiles", "/tmp");
    }

    /**
     * Here we varying the size of the bucket.
     * 
     * @throws Exception
     */
    @Test
    public void variyingBucketSize() throws Exception {
        allowedElementsPerBucket = 0;
        log.info("********************** Making test: Varying bucket size... *********************************");
        printOutParameterConfiguration();
        for (allowedElementsPerBucket = 500; allowedElementsPerBucket < 20000; allowedElementsPerBucket += 500) {
            log.info("======================= One test configuration ==========================");
            log.info("Bucket size is " + allowedElementsPerBucket);
            // Here we start one single test with incrementally adding more and more elements.
            incrementallyAddingOfEntries();
        }
    }

    /**
     * Here we varying the number of buffer threads that gets the buckets to synchronize it with the HDD.
     * 
     * @throws Exception
     */
    @Test
    public void varyingNumberOfThreads() throws Exception {
        numberOfThreads = 0;
        log.info("*************************** Making test: Varying number of threads... *********************************");
        printOutParameterConfiguration();
        for (numberOfThreads = 1; numberOfThreads < 10; numberOfThreads++) {
            log.info("======================= One test configuration ==========================");
            log.info("Number of threads is " + numberOfThreads);
            // Here we start one single test with incrementally adding more and more elements.
            incrementallyAddingOfEntries();
        }
    }

    /**
     * Here we varying the size of the waiting queue.
     * 
     * @throws Exception
     */
    @Test
    public void varyingSizeOfWaitingQueue() throws Exception {
        sizeOfWaitingQueue = 100;
        log.info("*************************** Making test: Varying number of threads... *********************************");
        printOutParameterConfiguration();
        for (sizeOfWaitingQueue = 1; sizeOfWaitingQueue < 10000; sizeOfWaitingQueue += 100) {
            log.info("======================= One test configuration ==========================");
            log.info("Size of waiting queue is " + sizeOfWaitingQueue);
            // Here we start one single test with incrementally adding more and more elements.
            incrementallyAddingOfEntries();
        }
    }
    
    /**
     * Here we varying the number of the buckets.
     * 
     * @throws Exception
     */
    @Test
    public void varyingNumberOfBuckets() throws Exception {
        numberOfBuckets = 1;
        log.info("*************************** Making test: Varying number of buckets... *********************************");
        printOutParameterConfiguration();
        for (numberOfBuckets = 1; numberOfBuckets < Math.pow(2, 16); numberOfBuckets += numberOfBuckets * 4) {
            log.info("======================= One test configuration ==========================");
            log.info("Number of buckets is " + numberOfBuckets);
            // Here we start one single test with incrementally adding more and more elements.
            incrementallyAddingOfEntries();
        }
    }

    /**
     * A new test session. An insertion of test data will be made. The insertion contains
     * <code>numberOfElementsToAdd</code> in one increment. The number of increments is <code>numberOfIncrements</code>.
     * 
     * @throws Exception
     */
    public void incrementallyAddingOfEntries() throws Exception {
        /* We make a new test session so we make sure that all former test data will be deleted. */
        cleanUp();
        long startTime = System.currentTimeMillis();
        long numberOfEntriesAlreadyInStorage = 0;
        for (int actualIncrement = 0; actualIncrement < numberOfIncrements; actualIncrement++) {
            log.info("--------------- One increment --------------------------------");
            long startTimeIncrement = System.currentTimeMillis();
            makeTest();
            long endTimeIncrement = System.currentTimeMillis();

            NumberFormat numberFormat = NumberFormat.getInstance();
            numberFormat.setMaximumFractionDigits(3);
            long timeDifferenceIncrement = endTimeIncrement - startTimeIncrement;
            log.info(
                    "The time for one increment was {} milliseconds ({}). There were {} elements ({}) already in the storage and we add {} elements to the storage.",
                    new Object[] {timeDifferenceIncrement, numberFormat.format(timeDifferenceIncrement), numberOfEntriesAlreadyInStorage, numberFormat.format(numberOfEntriesAlreadyInStorage),
                            numberOfElementsToAdd });
            numberOfEntriesAlreadyInStorage = numberOfEntriesAlreadyInStorage + numberOfElementsToAdd;
        }
        long endTime = System.currentTimeMillis();
        log.info("The overall time of the test was {}", (endTime - startTime));
    }

    /**
     * Makes the performance test with the configured values.
     * 
     * @throws BucketContainerException
     * @throws InterruptedException
     * @throws Exception
     */
    private void makeTest() throws BucketContainerException,
            InterruptedException, Exception {
        FirstBitHashFunction hashFunction = new FirstBitHashFunction(numberOfBuckets);
        Bucket<DummyKVStorable>[] buckets = new Bucket[hashFunction.getNumberOfBuckets()];
        for (int i = 0; i < buckets.length; i++) {
            buckets[i] = new Bucket<DummyKVStorable>(i, new DummyKVStorable());
        }

        System.out.println(directoryOfFiles);
        BucketContainer<DummyKVStorable> bucketContainer = new BucketContainer<DummyKVStorable>(buckets, sizeOfWaitingQueue, hashFunction);
        SyncManager<DummyKVStorable> buffer = new SyncManager<DummyKVStorable>(bucketContainer, numberOfThreads, directoryOfFiles,
                new SynchronizerFactory<DummyKVStorable>(new DummyKVStorable()));
        buffer.start();

        int numberOfLinkDataPerGeneration = (int) 1e5;
        long startTimeOneTest = System.currentTimeMillis();
        for (int i = 0; i < numberOfElementsToAdd; i += numberOfLinkDataPerGeneration) {
            DummyKVStorable[] testdata = TestUtils.generateTestdata(numberOfLinkDataPerGeneration, Long.MAX_VALUE, (long) 1e8);

            // long startTimeWriting = System.currentTimeMillis();
            bucketContainer.addToCache(testdata);
            // long endTimeWriting = System.currentTimeMillis();
            // log.info("Writing {} elements takes {} milliseconds.", testdata.length,
            // (endTimeWriting - startTimeWriting));
        }
        buffer.shutdown();
        buffer.join();
        long endTimeOneTest = System.currentTimeMillis();
        log.info("One test takes {} milliseconds.", (endTimeOneTest - startTimeOneTest));
    }

    private void cleanUp() {
        Iterator<File> files = FileUtils.iterateFiles(new File(directoryOfFiles), new String[] { "db" }, false);
        while (files.hasNext()) {
            files.next().delete();
        }
    }

    private void printOutParameterConfiguration() {
        log.info("Actual parameter configuration:");
        log.info("numberOfIncrements: " + numberOfIncrements);
        log.info("numberOfElementsToAdd: " + numberOfElementsToAdd);
        log.info("numberOfBuckets: " + numberOfBuckets);
        log.info("sizeOfWaitingQueue: " + sizeOfWaitingQueue);
        log.info("numberOfThreads: " + numberOfThreads);
        log.info("numberOfChunksToRead: " + numberOfChunksToRead);
        log.info("allowedElementsPerBucket: " + allowedElementsPerBucket);
        log.info("directoryOfFiles: " + directoryOfFiles);
    }
}
