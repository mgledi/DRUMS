package com.unister.semweb.superfast.storage.api;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.unister.semweb.superfast.storage.bucket.hashfunction.AbstractHashFunction;
import com.unister.semweb.superfast.storage.bucket.hashfunction.RangeHashFunction;
import com.unister.semweb.superfast.storage.file.AbstractHeaderFile.AccessMode;
import com.unister.semweb.superfast.storage.file.HeaderIndexFile;
import com.unister.semweb.superfast.storage.storable.DummyKVStorable;

/** Tests the SDrum API. */
public class SDrumTest {
    private static final Logger log = LoggerFactory.getLogger(SDrumTest.class);

    private static final String databaseDirectory = "/tmp/createTable/db";
    private static final String configurationFile = databaseDirectory + "/" + "database.properties";
    private static final int sizeOfMemoryBuckets = 1000;
    private static final int preQueueSize = 10000;
    private static final int numberOfSynchronizerThreads = 1;
    private AbstractHashFunction hashFunction;
    private DummyKVStorable prototype;

    @Before
    public void initialise() throws Exception {
        long[] ranges = new long[] { 0, 10, 20, 30 };
        String[] filenames = new String[] { "1.db", "2.db", "3.db", "4.db" };

        FileUtils.deleteQuietly(new File(databaseDirectory));

        hashFunction = new RangeHashFunction(ranges, filenames);
        prototype = new DummyKVStorable();
    }

    /**
     * Creates a table and inserts 10 elements. After that the file in which the data should be written are read and the
     * data is compared with the generated data.
     */
    @Test
    public void createTableAndInsertTest() throws Exception {
        // Adding elements to the drum.
        DummyKVStorable[] test = createDummyData(10);
        SDRUM<DummyKVStorable> table = SDRUM.createTable(configurationFile, databaseDirectory, sizeOfMemoryBuckets,
                preQueueSize, numberOfSynchronizerThreads, hashFunction, prototype);
        table.insertOrMerge(test);
        table.close();

        Thread.sleep(1000);

        List<DummyKVStorable> readData = readFrom(databaseDirectory + "/2.db", 10);

        // Comparing of the expected data with the written data.
        List<DummyKVStorable> testData = Arrays.asList(test);
        Assert.assertTrue(equals(testData, readData));
    }

    /**
     * Adds {@link DummyKVStorable}s to the DRUM that have different ranges.
     * 
     * @throws Exception
     */
    @Test
    public void insertDifferentRanges() throws Exception {
        List<DummyKVStorable> expectedFirstRange = new ArrayList<DummyKVStorable>();
        DummyKVStorable firstRange = createLinkData(5, 1, 0.5);
        expectedFirstRange.add(firstRange);

        DummyKVStorable secondRange = createLinkData(10, 12, 0.3);
        expectedFirstRange.add(secondRange);

        List<DummyKVStorable> expectedThirdRange = new ArrayList<DummyKVStorable>();
        DummyKVStorable thirdRange = createLinkData(29, 9, 0.23);
        expectedThirdRange.add(thirdRange);

        DummyKVStorable[] toAdd = new DummyKVStorable[] { firstRange, secondRange, thirdRange };

        SDRUM<DummyKVStorable> table = SDRUM.createTable(configurationFile, databaseDirectory, sizeOfMemoryBuckets,
                preQueueSize, numberOfSynchronizerThreads, hashFunction, prototype);
        table.insertOrMerge(toAdd);
        table.close();

        Thread.sleep(1000);

        List<DummyKVStorable> firstFileRead = readFrom(databaseDirectory + "/2.db", 2);
        List<DummyKVStorable> thirdFileRead = readFrom(databaseDirectory + "/4.db", 1);

        Assert.assertTrue(equals(expectedFirstRange, firstFileRead));
        Assert.assertTrue(equals(expectedThirdRange, thirdFileRead));
    }

    /**
     * Adds one element to the DRUM and select this element.
     * 
     * @throws Exception
     */
    @Test
    public void selectTestSingleElement() throws Exception {
        List<DummyKVStorable> dataList = new ArrayList<DummyKVStorable>();
        DummyKVStorable data = createLinkData(1, 1, 0.23);
        dataList.add(data);
        DummyKVStorable[] toAdd = new DummyKVStorable[] { data };

        SDRUM<DummyKVStorable> table = SDRUM.createTable(configurationFile, databaseDirectory, sizeOfMemoryBuckets,
                preQueueSize, numberOfSynchronizerThreads, hashFunction, prototype);
        table.insertOrMerge(toAdd);
        Thread.sleep(2000);

        List<DummyKVStorable> selectedData = table.select(1);

        Assert.assertTrue(equals(dataList, selectedData));
    }

    /**
     * Add elements from different ranges to the drum and select these elements.
     * 
     * @throws Exception
     */
    @Test
    public void selectTestSeveralRanges() throws Exception {
        List<DummyKVStorable> rangeData = new ArrayList<DummyKVStorable>();
        DummyKVStorable firstRange = createLinkData(2, 2, 0.24);
        DummyKVStorable secondRange = createLinkData(10, 10, 0.23);
        DummyKVStorable thirdRange = createLinkData(12, 19, 0.29);
        rangeData.add(firstRange);
        rangeData.add(secondRange);
        rangeData.add(thirdRange);

        DummyKVStorable[] toAdd = new DummyKVStorable[] { firstRange, secondRange, thirdRange };

        SDRUM<DummyKVStorable> table = SDRUM.createTable(configurationFile, databaseDirectory, sizeOfMemoryBuckets,
                preQueueSize, numberOfSynchronizerThreads, hashFunction, prototype);
        table.insertOrMerge(toAdd);

        Thread.sleep(1000);

        List<DummyKVStorable> selectedData = table.select(12, 10, 2);

        Assert.assertTrue(equalsWithoutOrder(rangeData, selectedData));
    }

    /**
     * Adds an element to the drum and read it from the right bucket.
     * 
     * @throws Exception
     */
    @Test
    public void readTestSingleElement() throws Exception {
        DummyKVStorable testElement = createLinkData(1, 2, 0.23);
        DummyKVStorable[] toAdd = new DummyKVStorable[] { testElement };
        List<DummyKVStorable> expectedData = Arrays.asList(toAdd);

        SDRUM<DummyKVStorable> table = SDRUM.createTable(configurationFile, databaseDirectory, sizeOfMemoryBuckets,
                preQueueSize, numberOfSynchronizerThreads, hashFunction, prototype);
        table.insertOrMerge(toAdd);

        Thread.sleep(1000);

        List<DummyKVStorable> readData = table.read(1, 0, 10);
        Assert.assertTrue(equalsWithoutOrder(expectedData, readData));
    }

    /**
     * Writes several elements of the same range to the drum and reads the drum.
     * 
     * @throws Exception
     */
    @Test
    public void readTestSeveralElements() throws Exception {
        DummyKVStorable[] testdata = createDummyData(10);

        SDRUM<DummyKVStorable> table = SDRUM.createTable(configurationFile, databaseDirectory, sizeOfMemoryBuckets,
                preQueueSize, numberOfSynchronizerThreads, hashFunction, prototype);
        table.insertOrMerge(testdata);

        Thread.sleep(1000);

        List<DummyKVStorable> readData = table.read(1, 0, 10);
        Assert.assertTrue(equalsWithoutOrder(Arrays.asList(testdata), readData));
    }

    /**
     * Writes several elements of the same range to the drum and reads the drum.
     * 
     * @throws Exception
     */
    @Test
    public void readTestSeveralElementsDifferentElementOffset() throws Exception {
        DummyKVStorable[] testdata = createDummyData(10);

        SDRUM<DummyKVStorable> table = SDRUM.createTable(configurationFile, databaseDirectory, sizeOfMemoryBuckets,
                preQueueSize, numberOfSynchronizerThreads, hashFunction, prototype);
        table.insertOrMerge(testdata);

        Thread.sleep(1000);

        List<DummyKVStorable> readData = table.read(1, 2, 10);

        List<DummyKVStorable> testdataList = Arrays.asList(testdata);
        List<DummyKVStorable> expectedData = testdataList.subList(2, testdataList.size());
        Assert.assertTrue(equalsWithoutOrder(expectedData, readData));
    }

    /** Add test data of different ranges to the DRUM and read the corresponding buckets. */
    @Test
    public void readTestDifferentRanges() throws Exception {
        DummyKVStorable[] testdata = createDummyData(1, 5);
        DummyKVStorable[] secondRange = createDummyData(11, 19);
        DummyKVStorable[] thirdRange = createDummyData(21, 29);

        DummyKVStorable[] completeTestdata = ArrayUtils.addAll(testdata, secondRange);
        completeTestdata = ArrayUtils.addAll(completeTestdata, thirdRange);

        SDRUM<DummyKVStorable> table = SDRUM.createTable(configurationFile, databaseDirectory, sizeOfMemoryBuckets,
                preQueueSize, numberOfSynchronizerThreads, hashFunction, prototype);
        table.insertOrMerge(completeTestdata);

        Thread.sleep(2000);

        List<DummyKVStorable> readSecondBucket = table.read(1, 0, 7);
        List<DummyKVStorable> readThirdBucket = table.read(2, 0, 10);
        List<DummyKVStorable> readFourthBucket = table.read(3, 0, 10);

        Assert.assertTrue(equalsWithoutOrder(Arrays.asList(testdata), readSecondBucket));
        Assert.assertTrue(equalsWithoutOrder(Arrays.asList(secondRange), readThirdBucket));
        Assert.assertTrue(equalsWithoutOrder(Arrays.asList(thirdRange), readFourthBucket));

    }

    /* ***************************************************************************** */
    /* ***************************************************************************** */
    /* ***************************** Helper methods ******************************** */
    /* ***************************************************************************** */
    /* ***************************************************************************** */

    /** Reads from the given file <code>numberOfElementsToRead</code> elements. */
    private List<DummyKVStorable> readFrom(String filename, int numberOfElementsToRead) throws Exception {
        HeaderIndexFile<DummyKVStorable> file = new HeaderIndexFile<DummyKVStorable>(filename, AccessMode.READ_ONLY, 1,
                prototype.getByteBufferSize());
        ByteBuffer dataBuffer = ByteBuffer.allocate(numberOfElementsToRead * prototype.getByteBufferSize());
        file.read(0, dataBuffer);
        dataBuffer.flip();

        List<DummyKVStorable> readData = new ArrayList<DummyKVStorable>();
        while (dataBuffer.position() < dataBuffer.limit()) {
            byte[] oneLinkData = new byte[prototype.getByteBufferSize()];
            dataBuffer.get(oneLinkData);
            DummyKVStorable oneDate = new DummyKVStorable(ByteBuffer.wrap(oneLinkData));
            readData.add(oneDate);
        }
        file.close();

        return readData;
    }

    /* ------------------------------------------------------- */
    /* ------------------Test data creation ------------------ */
    /* ------------------------------------------------------- */
    /** Creates <code>numberOfData</code> dummy {@link DummyKVStorable}. */
    private DummyKVStorable[] createDummyData(int numberOfData) {
        DummyKVStorable[] result = new DummyKVStorable[numberOfData];
        for (int i = 0; i < numberOfData; i++) {
            DummyKVStorable newData = createLinkData(i + 1, i, 1d / i);
            result[i] = newData;
        }
        return result;
    }

    /**
     * Creates a set of {@link DummyKVStorable}. There are (lastKey - firstKey) {@link DummyKVStorable} be generated. The first
     * {@link DummyKVStorable} has as key the <code>firstKey</code>, the last {@link DummyKVStorable} the <code>lastKey</code>.
     * 
     * @param firstKey
     * @param lastKey
     * @return
     */
    private DummyKVStorable[] createDummyData(int firstKey, int lastKey) {
        DummyKVStorable[] result = new DummyKVStorable[lastKey - firstKey];
        for (int i = firstKey; i < lastKey; i++) {
            DummyKVStorable oneDate = createLinkData(i, i + 1, 1d / i);
            result[i - firstKey] = oneDate;
        }
        return result;
    }

    /** Creates a specific {@link DummyKVStorable} with the given key, parentCount and relevanceScore. */
    private DummyKVStorable createLinkData(long key, int parentCount, double relevanceScore) {
        DummyKVStorable result = new DummyKVStorable();
        result.setKey(key);
        result.setParentCount(parentCount);
        result.setRelevanceScore(relevanceScore);
        return result;
    }

    /* ------------------------------------------------------- */
    /* ------------------Test data comparison ------------------ */
    /* ------------------------------------------------------- */

    /**
     * Compares to list with {@link DummyKVStorable} with each other. The compare criteria are key, parent count and relevance
     * score.
     */
    private boolean equals(List<DummyKVStorable> firstCompare, List<DummyKVStorable> secondCompare) {
        if (firstCompare.size() != secondCompare.size()) {
            log.error("Equals method: the first and second list have unequal size, first list {}, second list {}",
                    firstCompare.size(), secondCompare.size());
            logOut(firstCompare, secondCompare);
            return false;
        }

        for (int i = 0; i < firstCompare.size(); i++) {
            DummyKVStorable firstData = firstCompare.get(i);
            DummyKVStorable secondData = secondCompare.get(i);

            if (!equals(firstData, secondData)) {
                logOut(firstCompare, secondCompare);
                return false;
            }
        }
        return true;
    }

    /**
     * Compares two list of {@link DummyKVStorable}s with each other. The order of the elements within the lists is not
     * respected.
     */
    private boolean equalsWithoutOrder(List<DummyKVStorable> firstCompare, List<DummyKVStorable> secondCompare) {
        if (firstCompare.size() != secondCompare.size()) {
            log.error("The sizes of the two lists are not equal. First list: {}, second list: {}", firstCompare.size(),
                    secondCompare.size());
            return false;
        }
        boolean wasFound = false;
        for (DummyKVStorable dataFirst : firstCompare) {

            for (DummyKVStorable dataSecond : secondCompare) {

                // If we have a found the second element within the first list we notify that with the "wasFound"
                // variable.
                if (equals(dataFirst, dataSecond)) {
                    wasFound = true;
                    break;
                }
            }

            // If the element was not found we return false.
            if (!wasFound) {
                return false;
            }

            wasFound = false;
        }
        return true;
    }

    /**
     * Comapares two {@link DummyKVStorable} with each other. The relevant field that are compared are: key, parentCount and
     * relevanceScore.
     */
    private boolean equals(DummyKVStorable firstData, DummyKVStorable secondData) {
        if (firstData.key != secondData.key) {
            log.error("The orders of the two lists is wrong. First element key: {}, second element key {}",
                    firstData.key, secondData.key);
            return false;
        }

        if (firstData.getParentCount() != secondData.getParentCount()) {
            log.error(
                    "The orders of the two lists is wrong. First element parent count: {}, second element parent count {}",
                    firstData.getParentCount(), secondData.getParentCount());
            return false;
        }

        if (firstData.getRelevanceScore() != secondData.getRelevanceScore()) {
            log.error(
                    "The orders of the two lists is wrong. First element relevance score: {}, second element relevance score {}",
                    firstData.getRelevanceScore(), secondData.getRelevanceScore());
            return false;
        }
        return true;
    }

    /* ------------------------------------------------------- */
    /* ------------------Test data output -------------------- */
    /* ------------------------------------------------------- */

    private void logOut(List<DummyKVStorable> firstList, List<DummyKVStorable> secondList) {
        log.info("================ Elements of the first list ===================");
        for (DummyKVStorable oneDate : firstList) {
            log.info("***************** One date ********************");
            log.info("Key: {}", oneDate.key);
            log.info("Parent count: {}", oneDate.getParentCount());
            log.info("Relevance score: {}", oneDate.getRelevanceScore());
        }

        log.info("======================== Elements of the second list ==================");
        for (DummyKVStorable oneDate : secondList) {
            log.info("***************** One date ********************");
            log.info("Key: {}", oneDate.key);
            log.info("Parent count: {}", oneDate.getParentCount());
            log.info("Relevance score: {}", oneDate.getRelevanceScore());
        }
    }
}
