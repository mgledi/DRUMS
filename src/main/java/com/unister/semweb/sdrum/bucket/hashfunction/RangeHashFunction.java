package com.unister.semweb.sdrum.bucket.hashfunction;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.com.bytecode.opencsv.CSVReader;

import com.unister.semweb.sdrum.bucket.hashfunction.util.RangeHashSorter;
import com.unister.semweb.sdrum.storable.KVStorable;

/**
 * This hashFunction maps an element dependent on its range-bucket. It checks if given key falls in a specific range.
 * 
 * @author m.gleditzsch
 */
public class RangeHashFunction extends AbstractHashFunction {
    private static final Logger log = LoggerFactory.getLogger(RangeHashFunction.class);

    private static final long serialVersionUID = 1197410536026296596L;

    /** the file where the hashfunction is stored human-readable */
    private File hashFunctionFile;

    private long[] maxRangeValues;
    private int[] bucketIds;
    private String[] filenames;

    private RangeHashSorter sortMachine;

    /**
     * This method instantiates a new {@link RangeHashFunction} by the given rangeValues. The given array should contain
     * only the maximal allowed value per bucket. The minimal value will be the direct successor of the previous maximal
     * value. Remember: the array will be handled circular.
     * 
     * @param rangeValues
     *            the maximum keys for all buckets
     * @param filenames
     *            the filenames for all buckets
     * @param bucketSizes
     *            the sizes for all buckets
     * @param file
     *            the file where to store the hashfunction
     */
    public RangeHashFunction(long[] rangeValues, String[] filenames, int[] bucketSizes, File file) {
        this.hashFunctionFile = file;
        this.buckets = rangeValues.length;
        this.maxRangeValues = rangeValues;
        this.filenames = filenames;

        if (bucketSizes == null) {
            this.bucketSizes = new int[maxRangeValues.length];
            Arrays.fill(this.bucketSizes, INITIAL_BUCKET_SIZE);
        } else {
            this.bucketSizes = bucketSizes;
        }

        sortMachine = new RangeHashSorter(maxRangeValues, filenames, this.bucketSizes);
        sortMachine.quickSort();
        generateBucketIds();
    }

    /**
     * This method instantiates a new {@link RangeHashFunction} by the given {@link File}. The File contains some long
     * values, which describe the maximal allowed values for the buckets. The minimal value will be the direct successor
     * of the previous maximal value. Remember: the array will be handled circular.
     * 
     * @param long[] rangeValues
     */
    public RangeHashFunction(File file) throws IOException {
        this.hashFunctionFile = file;
        FileReader configFile = new FileReader(file);
        CSVReader csvReader = new CSVReader(configFile, ' ');

        List<String[]> readData = csvReader.readAll();

        maxRangeValues = new long[readData.size()];
        filenames = new String[readData.size()];
        bucketSizes = new int[readData.size()];

        for (int i = 0; i < readData.size(); i++) {
            String[] oneCSVLine = readData.get(i);
            try {
                maxRangeValues[i] = Long.valueOf(oneCSVLine[0]);
                filenames[i] = oneCSVLine[1];
                bucketSizes[i] = INITIAL_BUCKET_SIZE; // TODO: adapt file
            } catch (NullPointerException ex) {
                log.error(
                        "The csv file has not the expected format. The format is: range bucketId filename. One column is missing",
                        ex);
                throw new IOException(ex);
            } catch (ClassCastException ex) {
                log.error(
                        "The csv file has not the expected format. The format is: range bucketId filename. One column has the wrong type.",
                        ex);
                throw new IOException(ex);
            }
        }

        sortMachine = new RangeHashSorter(maxRangeValues, filenames, bucketSizes);
        sortMachine.quickSort();
        generateBucketIds();

    }

    /**
     * Returns the File, where the HashFunction is stored human-readable
     * 
     * @return File
     */
    public File getHashFunctionFile() {
        return this.hashFunctionFile;
    }

    /**
     * generates the correct index structure, namely the bucketIds to the already initialized filenames and
     * maxRangeValues
     */
    private void generateBucketIds() {
        // generate indexes for buckets, needed if two different ranges belong to the same file
        this.buckets = 0;
        bucketIds = new int[filenames.length];

        HashMap<String, Integer> tmpSeenFilenames = new HashMap<String, Integer>();
        for (int i = 0; i < filenames.length; i++) {
            if (!tmpSeenFilenames.containsKey(filenames[i])) {
                tmpSeenFilenames.put(filenames[i], this.buckets++);
            }
            bucketIds[i] = tmpSeenFilenames.get(filenames[i]);
        }
    }

    /** Returns the size of the bucket with the given bucket-id. */
    public long getMaxRange(int bucketId) {
        return maxRangeValues[bucketId];
    }

    /** Gets the bucket id to the given <code>key</code>. */
    @Override
    public int getBucketId(long key) {
        int index = searchBucketIndex(key, 0, maxRangeValues.length - 1);
        return bucketIds[index];
    }

    /**
     * Searches for the index in maxRangeValues for the given <code>key</code>. This is done by a binary search. So we
     * need the left and right index. Remeber: this may not be the bucketId
     */
    public int searchBucketIndex(long key, int leftIndex, int rightIndex) {
        if (key > maxRangeValues[rightIndex]) {
            return 0;
        }

        while (leftIndex <= rightIndex) {
            int midIndex = ((rightIndex - leftIndex) / 2) + leftIndex;
            if (midIndex == 0 || maxRangeValues[midIndex - 1] < key && key <= maxRangeValues[midIndex]) {
                return midIndex;
            } else if (key > maxRangeValues[midIndex]) {
                leftIndex = midIndex + 1;
            } else {
                rightIndex = midIndex - 1;
            }
        }

        log.error("Could not find a bucket for key {}", key);
        return -1;

    }

    /** Gets the bucket id from the given date. */
    @Override
    public int getBucketId(KVStorable<?> key) {
        return getBucketId(key.getKey());
    }

    /** Get the file name of the given bucket. */
    @Override
    public String getFilename(int bucketId) {
        return filenames[bucketId];
    }

    //TODO Must be tested.
    /**
     * Writes the hash function, represented as tuples (range, filename) into the file that is linked with the
     * HashFunction. The content of the file is overwritten.
     */
    public void writeToFile() throws IOException {
        FileWriter fileWriter = new FileWriter(hashFunctionFile);
        BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
        for (int i = 0; i < maxRangeValues.length; i++) {
            String oneCSVLine = makeOneLine(maxRangeValues[i], filenames[i]);
            bufferedWriter.append(oneCSVLine);
        }

        bufferedWriter.flush();
        bufferedWriter.close();
    }

    /**
     * Concatenates the given range value and the file name to one string. It is used to write the hash function file.
     */
    private String makeOneLine(long oneRange, String filename) {
        StringBuffer buffer = new StringBuffer();
        buffer.append(oneRange).append(" ").append(filename).append('\n');
        return buffer.toString();
    }
}
