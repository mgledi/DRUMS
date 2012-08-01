package com.unister.semweb.sdrum.api;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.unister.semweb.sdrum.TestUtils;
import com.unister.semweb.sdrum.bucket.SortMachine;
import com.unister.semweb.sdrum.bucket.hashfunction.AbstractHashFunction;
import com.unister.semweb.sdrum.bucket.hashfunction.RangeHashFunction;
import com.unister.semweb.sdrum.storable.DummyKVStorable;
import com.unister.semweb.sdrum.utils.KeyUtils;

/** Tests the SDrum API. */
public class SDrumTest {
    private static final Logger log = LoggerFactory.getLogger(SDrumTest.class);
    private AbstractHashFunction hashFunction;

    @Before
    public void initialise() throws Exception {
        long[] ranges = new long[] { 0, 10, 20, 30 };
        byte[][] bRanges = KeyUtils.transformToByteArray(ranges);
        String[] filenames = new String[] { "1.db", "2.db", "3.db", "4.db" };
        FileUtils.deleteQuietly(new File(TestUtils.gp.databaseDirectory));

        hashFunction = new RangeHashFunction(bRanges, filenames, new File("/tmp/hash.hs"));
    }

    /**
     * This method is for testing the binary search in SDRUM.findElementInReadBuffer()
     * 
     * @throws IOException
     * @throws ClassNotFoundException
     */
    @Test
    public void findElementInReadBufferTest() throws IOException, ClassNotFoundException {
        log.info("Test Binary search. findElementInReadBufferTest()");
        SDRUM<DummyKVStorable> table = SDRUM_API.createOrOpenTable(hashFunction, TestUtils.gp);
        // create data
        DummyKVStorable d1 = DummyKVStorable.getInstance();
        d1.setKey(new byte[] { 0, 0, 0, 0, 0, 0, 0, 1 });
        DummyKVStorable d2 = DummyKVStorable.getInstance();
        d2.setKey(new byte[] { 0, 0, 0, 0, 0, 0, 0, 2 });
        DummyKVStorable d3 = DummyKVStorable.getInstance();
        d3.setKey(new byte[] { 0, 0, 0, 0, 0, 0, 0, 3 });
        ByteBuffer bb = ByteBuffer.allocate(3 * TestUtils.gp.elementSize);
        bb.put(d1.toByteBuffer().array());
        bb.put(d2.toByteBuffer().array());
        bb.put(d3.toByteBuffer().array());

        Assert.assertEquals(-1, table.findElementInReadBuffer(bb, new byte[] { 0, 0, 0, 0, 0, 0, 0, 0 }, 0));
        Assert.assertEquals(0, table.findElementInReadBuffer(bb, new byte[] { 0, 0, 0, 0, 0, 0, 0, 1 }, 0));
        Assert.assertEquals(1 * TestUtils.gp.elementSize,
                table.findElementInReadBuffer(bb, new byte[] { 0, 0, 0, 0, 0, 0, 0, 2 }, 0));
        Assert.assertEquals(2 * TestUtils.gp.elementSize,
                table.findElementInReadBuffer(bb, new byte[] { 0, 0, 0, 0, 0, 0, 0, 3 }, 0));
        Assert.assertEquals(2 * TestUtils.gp.elementSize,
                table.findElementInReadBuffer(bb, new byte[] { 0, 0, 0, 0, 0, 0, 0, 3 },
                        1 * TestUtils.gp.elementSize));
        Assert.assertEquals(2 * TestUtils.gp.elementSize,
                table.findElementInReadBuffer(bb, new byte[] { 0, 0, 0, 0, 0, 0, 0, 3 },
                        2 * TestUtils.gp.elementSize));
        Assert.assertEquals(-1, table.findElementInReadBuffer(bb, new byte[] { 0, 0, 0, 0, 0, 0, 0, 3 },
                3 * TestUtils.gp.elementSize));
    }

    /**
     * Creates a table and inserts 10 elements. After that the file in which the data should be written are read and the
     * data is compared with the generated data.
     */
    @Test
    public void createTableAndInsertTest() throws Exception {
        log.info("Test simple write to one Bucket. createTableAndInsertTest()");
        // Adding elements to the drum.
        DummyKVStorable[] test = TestUtils.createDummyData(10);
        SDRUM<DummyKVStorable> table = SDRUM_API.createTable(hashFunction, TestUtils.gp);
        table.insertOrMerge(test);
        table.close();
        List<DummyKVStorable> readData = TestUtils.readFrom(TestUtils.gp.databaseDirectory + "/2.db", 10);
        Assert.assertArrayEquals(test, readData.toArray(new DummyKVStorable[readData.size()]));
    }

    /**
     * Adds {@link DummyKVStorable}s to different Buckets.
     * 
     * @throws Exception
     */
    @Test
    public void insertDifferentRanges() throws Exception {
        log.info("Test extended write to different Buckets. createTableAndInsertTest()");
        DummyKVStorable bucket2_el1 = TestUtils.createDummyData(KeyUtils.convert(5), 1, 0.5);
        DummyKVStorable bucket2_el2 = TestUtils.createDummyData(KeyUtils.convert(10), 12, 0.3);
        DummyKVStorable bucket4_el1 = TestUtils.createDummyData(KeyUtils.convert(29), 9, 0.23);
        DummyKVStorable bucket4_el2 = TestUtils.createDummyData(KeyUtils.convert(30), 9, 0.23);

        DummyKVStorable[] toAdd = new DummyKVStorable[] { bucket2_el1, bucket2_el2, bucket4_el1, bucket4_el2 };

        SDRUM<DummyKVStorable> table = SDRUM_API.createTable(hashFunction, TestUtils.gp);
        table.insertOrMerge(toAdd);
        table.close();

        List<DummyKVStorable> db2 = TestUtils.readFrom(TestUtils.gp.databaseDirectory + "/2.db", 1000);
        List<DummyKVStorable> db4 = TestUtils.readFrom(TestUtils.gp.databaseDirectory + "/4.db", 1000);

        assertEquals(2, db2.size());
        assertEquals(bucket2_el1, db2.get(0));
        assertEquals(bucket2_el2, db2.get(1));
        assertEquals(2, db4.size());
        assertEquals(bucket4_el1, db4.get(0));
        assertEquals(bucket4_el2, db4.get(1));
    }

    /**
     * Adds one element to the DRUM and select this element.
     * 
     * @throws Exception
     */
    @Test
    public void selectTestSingleElement() throws Exception {
        DummyKVStorable data = TestUtils.createDummyData(KeyUtils.convert(1), 1, 0.23);
        DummyKVStorable[] toAdd = new DummyKVStorable[] { data };

        SDRUM<DummyKVStorable> table = SDRUM_API.createTable(hashFunction, TestUtils.gp);
        table.insertOrMerge(toAdd);
        table.close();
        
        List<DummyKVStorable> selectedData = table.select(1l);

        Assert.assertEquals(1, selectedData.size());
        Assert.assertEquals(data, selectedData.get(0));
    }

    /**
     * Add elements from different ranges to the SDRUM and select these elements.
     * 
     * @throws Exception
     */
    @Test
    public void selectTestSeveralRanges() throws Exception {
        DummyKVStorable firstRange = TestUtils.createDummyData(KeyUtils.convert(2), 2, 0.24);
        DummyKVStorable secondRange = TestUtils.createDummyData(KeyUtils.convert(10), 10, 0.23);
        DummyKVStorable thirdRange = TestUtils.createDummyData(KeyUtils.convert(12), 19,0.29);
        DummyKVStorable[] toAdd = new DummyKVStorable[] { firstRange, secondRange, thirdRange };

        SDRUM<DummyKVStorable> table = SDRUM_API.createTable(hashFunction, TestUtils.gp);
        table.insertOrMerge(toAdd);
        table.close();
        
        List<DummyKVStorable> selectedData = table.select(12, 10, 2);
        
        DummyKVStorable[] result = selectedData.toArray(new DummyKVStorable[selectedData.size()]);
        SortMachine.quickSort(toAdd);
        SortMachine.quickSort(result);
        Assert.assertArrayEquals(toAdd, result);
    }

    /**
     * Adds an element to the drum and read it from the right bucket.
     * 
     * @throws Exception
     */
    @Test
    public void readTestSingleElement() throws Exception {
        DummyKVStorable testElement = TestUtils.createDummyData(KeyUtils.convert(1), 2, 0.23);
        DummyKVStorable[] toAdd = new DummyKVStorable[] { testElement };
        
        SDRUM<DummyKVStorable> table = SDRUM_API.createTable(hashFunction, TestUtils.gp);
        table.insertOrMerge(toAdd);
        table.close();
            
        List<DummyKVStorable> selectedData = table.read(1, 0, 10);
        
        DummyKVStorable[] result = selectedData.toArray(new DummyKVStorable[selectedData.size()]);
        SortMachine.quickSort(toAdd);
        SortMachine.quickSort(result);
        Assert.assertArrayEquals(toAdd, result);
    }

    /**
     * Writes several elements of the same range to the drum and reads the drum.
     * 
     * @throws Exception
     */
    @Test
    public void readTestSeveralElements() throws Exception {
        DummyKVStorable[] testdata = TestUtils.createDummyData(10);

        SDRUM<DummyKVStorable> table = SDRUM_API.createTable(hashFunction, TestUtils.gp);
        table.insertOrMerge(testdata);
        table.close();

        List<DummyKVStorable> selectedData = table.read(1, 0, 10);

        DummyKVStorable[] result = selectedData.toArray(new DummyKVStorable[selectedData.size()]);
        SortMachine.quickSort(testdata);
        Assert.assertArrayEquals(testdata, result);
        
        List<DummyKVStorable> selectedData2 = table.read(1, 5, 10);
        DummyKVStorable[] result2 = selectedData2.toArray(new DummyKVStorable[selectedData2.size()]);
        SortMachine.quickSort(testdata);
        Assert.assertArrayEquals(Arrays.copyOfRange(testdata,5,10), result2);
    }


    /** Add test data of different ranges to the DRUM and read the corresponding buckets. */
    @Test
    public void readTestDifferentRanges() throws Exception {
        DummyKVStorable[] testdata = TestUtils.createDummyData(1, 5);
        DummyKVStorable[] secondRange = TestUtils.createDummyData(11, 19);
        DummyKVStorable[] thirdRange = TestUtils.createDummyData(21, 29);

        DummyKVStorable[] completeTestdata = TestUtils.merge(testdata, secondRange, thirdRange);

        SDRUM<DummyKVStorable> table = SDRUM_API.createTable(hashFunction, TestUtils.gp);
        table.insertOrMerge(completeTestdata);
        table.close();

        List<DummyKVStorable> readSecondBucket = table.read(1, 0, 7);
        List<DummyKVStorable> readThirdBucket = table.read(2, 0, 10);
        List<DummyKVStorable> readFourthBucket = table.read(3, 0, 10);

        Assert.assertArrayEquals(testdata, readSecondBucket.toArray(new DummyKVStorable[readSecondBucket.size()]));
        Assert.assertArrayEquals(secondRange, readThirdBucket.toArray(new DummyKVStorable[readThirdBucket.size()]));
        Assert.assertArrayEquals(thirdRange, readFourthBucket.toArray(new DummyKVStorable[readFourthBucket.size()]));
    }
    
    /** Add test data of different ranges to the DRUM and read the corresponding buckets. */
    @Test
    public void testMergeOfOneElement() throws Exception {
        DummyKVStorable date1 = TestUtils.createDummyData(KeyUtils.convert(5), 1, 0.5);
        DummyKVStorable[] completeTestdata = new DummyKVStorable[]{date1};
        
        SDRUM<DummyKVStorable> table = SDRUM_API.createTable(hashFunction, TestUtils.gp);
        table.insertOrMerge(completeTestdata);
        table.close();
        
        table = SDRUM_API.createOrOpenTable(hashFunction, TestUtils.gp);
        table.insertOrMerge(completeTestdata);
        table.close();
        List<DummyKVStorable> readSecondBucket = table.read(1, 0, 7);

        assertEquals(1, readSecondBucket.size());
        assertEquals(2, readSecondBucket.get(0).getValueAsInt("parentCount"));
    }
}
