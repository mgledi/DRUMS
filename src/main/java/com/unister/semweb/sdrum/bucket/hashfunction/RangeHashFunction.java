package com.unister.semweb.sdrum.bucket.hashfunction;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.unister.semweb.sdrum.bucket.hashfunction.util.RangeHashSorter;
import com.unister.semweb.sdrum.storable.AbstractKVStorable;
import com.unister.semweb.sdrum.utils.KeyUtils;

/**
 * This hashFunction maps an element dependent on its range-bucket. It checks if given key falls in a specific range.
 * 
 * @author m.gleditzsch
 */
public class RangeHashFunction extends AbstractHashFunction {
    private static final Logger log = LoggerFactory.getLogger(RangeHashFunction.class);

    /** the file where the hashfunction is stored human-readable */
    private File hashFunctionFile;

    /** the key composition. E.g. 2 4 2 8 or char int char long */
    private int keyComposition[];

    private byte[][] maxRangeValues;
    private int[] bucketIds;
    private String[] filenames;

    private RangeHashSorter sortMachine;

    /**
     * This constructor instantiates a new {@link RangeHashFunction} with the given number of ranges. It tries to size
     * all ranges equally.
     * 
     * @param ranges
     *            the number of ranges
     * @param keySize
     *            the size in bytes of the key
     * @param file
     *            the file where to store the hashfunction
     */
    public RangeHashFunction(int ranges, int keySize, File file /* TODO: prototype */) {
        this.hashFunctionFile = file;
        this.buckets = ranges;
        byte[] max = new byte[keySize];
        Arrays.fill(max, (byte) -1);
        try {
            this.maxRangeValues = KeyUtils.getRanges(new byte[keySize], max, keySize);
        } catch (Exception e) {
            e.printStackTrace();
        }
        this.filenames = new String[ranges];
        for (int i = 0; i < ranges; i++) {
            filenames[i] = i + ".db";
        }
        this.keySize = keySize;
        this.keyComposition = new int[keySize];
        Arrays.fill(keyComposition, 1);
        sort();
    }

    /**
     * This method instantiates a new {@link RangeHashFunction} by the given rangeValues. The given array should contain
     * only the maximal allowed value per bucket. The minimal value will be the direct successor of the previous maximal
     * value. Remember: the array will be handled circular.
     * 
     * @param rangeValues
     *            the maximum keys for all buckets
     * @param filenames
     *            the filenames for all buckets
     * @param file
     *            the file where to store the hashfunction
     */
    public RangeHashFunction(byte[][] rangeValues, String[] filenames, File file /* TODO: prototype */) {
        this.hashFunctionFile = file;
        this.buckets = rangeValues.length;
        this.maxRangeValues = rangeValues;
        this.filenames = filenames;
        this.keySize = rangeValues[0].length;
        this.keyComposition = new int[rangeValues[0].length];
        Arrays.fill(keyComposition, 1);
        sort();
    }

    /** Sorts the max range values corresponding to the file names and the bucket sizes. */
    private void sort() {
        sortMachine = new RangeHashSorter(maxRangeValues, filenames);
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
        List<String> readData = IOUtils.readLines(configFile);
        configFile.close();

        maxRangeValues = new byte[readData.size() - 1][];
        filenames = new String[readData.size() - 1];

        // analyze header
        String[] header = readData.get(0).split("\t");
        keySize = 0;
        keyComposition = new int[header.length - 1];
        for (int i = 0; i < keyComposition.length; i++) {
            int e = stringToByteCount(header[i]);
            if (e == 0) {
                throw new IOException("Header could not be read. Could not decode " + header[i]);
            }
            keyComposition[i] = e;
            keySize += e;
        }
        for (int i = 0; i < readData.size() - 1; i++) {
            String[] Aline = readData.get(i + 1).split("\t");
            // TODO: format exception
            maxRangeValues[i] = new byte[keySize];
            // we need an offset for the current part of the key
            int keyPartOffset = -1;
            for (int k = 0; k < keyComposition.length; k++) {
                long tmp = Long.parseLong(Aline[k]);
                // set the offset on the last byte of the current part of the key
                keyPartOffset += keyComposition[k];
                // start from the lowest bits of the read long value and use them for the last byte (= lowest byte) of
                // the current part of the key. Than take the next bits and the second lowest byte
                for (int b = 0; b < keyComposition[k]; b++) {
                    maxRangeValues[i][keyPartOffset - b] = (byte) tmp;
                    tmp = tmp >> 8;
                }
            }
            filenames[i] = Aline[keyComposition.length];
        }
        sortMachine = new RangeHashSorter(maxRangeValues, filenames);
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
        this.buckets = bucketIds.length;
    }

    /** Returns the maximal key in the bucket with the given bucketId. */
    public byte[] getMaxRange(int bucketId) {
        return maxRangeValues[bucketId];
    }

    /** Gets the bucket id to the given <code>key</code>. */
    @Override
    public int getBucketId(byte[] key) {
        int index = searchBucketIndex(key, 0, maxRangeValues.length - 1);
        return bucketIds[index];
    }

    /**
     * Searches for the index in maxRangeValues for the given <code>key</code>. This is done by a binary search. So we
     * need the left and right index. Remeber: this may not be the bucketId
     */
    public int searchBucketIndex(byte[] key, int leftIndex, int rightIndex) {
        if (KeyUtils.compareKey(key, maxRangeValues[rightIndex]) > 0) {
            return 0;
        }

        byte comp1, comp2;
        while (leftIndex <= rightIndex) {
            int midIndex = ((rightIndex - leftIndex) / 2) + leftIndex;
            comp2 = KeyUtils.compareKey(key, maxRangeValues[midIndex]);
            if (midIndex == 0) {
                if (comp2 > 0) {
                    return 1;
                }
                return 0;
            }
            comp1 = KeyUtils.compareKey(maxRangeValues[midIndex - 1], key);
            if (comp1 < 0 && comp2 <= 0) {
                return midIndex;
            } else if (comp2 > 0) {
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
    public int getBucketId(AbstractKVStorable key) {
        return getBucketId(key.getKey());
    }

    /** Get the file name of the given bucket. */
    @Override
    public String getFilename(int bucketId) {
        return filenames[bucketId];
    }

    // TODO Must be tested.
    /**
     * Writes the hash function, represented as tuples (range, filename) into the file that is linked with the
     * HashFunction. The content of the file is overwritten.
     */
    public void writeToFile() throws IOException {
        FileWriter fileWriter = new FileWriter(hashFunctionFile);
        BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
        for (int i = 0; i < maxRangeValues[0].length; i++) {
            bufferedWriter.append('b').append('\t');
        }
        bufferedWriter.append("filename").append('\t').append("\n");
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
    private String makeOneLine(byte[] value, String filename) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < value.length; i++) {
            sb.append(value[i]).append('\t');
        }
        sb.append(filename).append('\n');
        return sb.toString();
    }

    @Override
    public int getBucketId(String dbFilename) {
        for (int i = 0; i < filenames.length; i++) {
            if (filenames[i].equals(dbFilename)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Replaces one bucket line within the {@link RangeHashFunction} with the lines given. All added buckets are set to
     * the specified bucket size. If the <code>bucketId</code> that is to replaced is invalid a
     * {@link IllegalArgumentException} is thrown.
     * 
     * @param keysToInsert
     * @param bucketId
     * @return
     */
    public void replace(int bucketId, byte[][] keysToInsert) {
        if (bucketId < 0 || bucketId >= maxRangeValues.length) {
            throw new IllegalArgumentException("Invalid bucketId: " + bucketId);
        }
        int numberOfPartitions = keysToInsert.length;
        int newSize = this.getNumberOfBuckets() - 1 + numberOfPartitions;
        byte[][] newMaxRangeValues = new byte[newSize][];
        String[] newFileNames = new String[newSize];

        int k = 0;
        for (int i = 0; i < this.getNumberOfBuckets(); i++) {
            if (i != bucketId) {
                newMaxRangeValues[k] = this.getMaxRange(i);
                newFileNames[k] = this.getFilename(i);
                k++;
            }
        }
        for (int i = this.getNumberOfBuckets() - 1; i < newSize; i++) {
            k = i - (this.getNumberOfBuckets() - 1);
            newMaxRangeValues[i] = keysToInsert[k];
            newFileNames[i] = generateFileName(k, this.getFilename(bucketId));
        }

        this.maxRangeValues = newMaxRangeValues;
        this.filenames = newFileNames;

        sort();
    }

    /**
     * Returns the bucket ids for the given byte prefix. Example: Suppose you have the following ranges:
     * <ul>
     * <li>1 0 0 0 - 1 1 0 0</li>
     * <li>1 1 0 1 - 1 1 1 1</li>
     * <li>2 0 0 0 - 2 1 1 1</li>
     * </ul>
     * 
     * The prefix is 1 then the method will return the bucket ids of the first two ranges.
     * 
     * If the <code>prefix</code> has more elements than the keys within the hash function an
     * {@link IllegalArgumentException} is thrown.
     * 
     * @param prefix
     * @return
     */
    public int[] getBucketIdsFor(byte[] prefix) throws IllegalArgumentException {
        List<Integer> intermediateResult = new ArrayList<Integer>();

        int keySize = maxRangeValues[0].length;

        byte[] begin = Arrays.copyOf(prefix, keySize);
        byte[] end = Arrays.copyOf(prefix, keySize);
        Arrays.fill(end, prefix.length, keySize, (byte) 255);

        byte[] leftKey = new byte[keySize];
        byte[] rightKey = new byte[keySize];

        for (int i = 0; i < maxRangeValues.length; i++) {
            rightKey = maxRangeValues[i];

            int compareLeftBegin = KeyUtils.compareKey(leftKey, begin);
            int compareBeginRight = KeyUtils.compareKey(begin, rightKey);

            int compareLeftEnd = KeyUtils.compareKey(leftKey, end);
            int compareEndRight = KeyUtils.compareKey(end, rightKey);

            int compareBeginLeft = KeyUtils.compareKey(begin, leftKey);

            if ((compareLeftBegin < 0 && compareBeginRight <= 0) || (compareLeftEnd < 0 && compareEndRight <= 0)
                    || (compareBeginLeft <= 0 && compareLeftEnd < 0)) {
                /* Before we determine the bucket id of the left key we add 1 to the key. */
                byte[] incrementedLeftKey = increment(leftKey);
                intermediateResult.add(getBucketId(incrementedLeftKey));
            }
            leftKey = rightKey;
        }

        /* This handles the last range if the prefix lies within the last range. */
        byte[] lastRangeValue = maxRangeValues[maxRangeValues.length - 1];
        int compareLastBegin = KeyUtils.compareKey(lastRangeValue, begin);
        int compareLastEnd = KeyUtils.compareKey(lastRangeValue, end);

        byte[] nullHash = new byte[keySize];

        if (compareLastBegin < 0 || compareLastEnd < 0 || Arrays.equals(begin, nullHash)
                || Arrays.equals(end, nullHash)) {
            intermediateResult.add(0);
        }

        int[] result = new int[intermediateResult.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = intermediateResult.get(i);
        }
        return result;
    }

    /** Increment the given value by one. */
    private byte[] increment(byte[] toIncrement) {
        byte[] increment = new byte[toIncrement.length];
        increment[increment.length - 1] = 1;

        try {
            byte[] incrementedResult = KeyUtils.sumUnsigned(toIncrement, increment);
            return incrementedResult;

        } catch (Exception ex) {
            log.error("Could not increment number: {}", Arrays.toString(toIncrement), ex);
        }
        return toIncrement;
    }

    /**
     * Returns the ranges of this hash function.
     * 
     * @return
     */
    public byte[][] getRanges() {
        return this.maxRangeValues;
    }

    /**
     * generates a new filename for a subbucket from the given oldName
     * 
     * @param subBucket
     * @param oldName
     * @return
     */
    protected String generateFileName(int subBucket, String oldName) {
        int dotPos = oldName.lastIndexOf(".");
        int slashPos = Math.max(oldName.lastIndexOf("/"), oldName.lastIndexOf("\\"));
        String prefix;
        String suffix;
        if (dotPos > slashPos) {

            prefix = oldName.substring(0, dotPos);
            suffix = oldName.substring(dotPos);
        } else {
            prefix = oldName;
            suffix = "";
        }
        return prefix + "_" + subBucket + suffix;
    }

    /**
     * Makes a copy of the current {@link RangeHashFunction}. <b>Note: the file name is also copied. Make sure that you
     * don't overwrite the file if you change one of the functions.</b>
     * 
     * @return
     */
    public RangeHashFunction copy() {
        RangeHashFunction clone = new RangeHashFunction(maxRangeValues, filenames, hashFunctionFile);
        return clone;
    }

    /**
     * The header of could contain characters which are not numbers. Some of them can be translated into bytes. E.g.
     * char would be two byte.
     */
    public static int stringToByteCount(String code) {
        @SuppressWarnings("serial")
        HashMap<String, Integer> codingMap = new HashMap<String, Integer>() {
            {
                put("b", 1);
                put("byte", 1);
                put("bool", 1);
                put("boolean", 1);
                put("c", 2);
                put("char", 2);
                put("character", 2);
                put("i", 4);
                put("int", 4);
                put("integer", 4);
                put("f", 4);
                put("float", 4);
                put("d", 8);
                put("double", 8);
                put("l", 8);
                put("long", 8);
                put("1", 1);
                put("2", 2);
                put("3", 3);
                put("4", 4);
                put("5", 5);
                put("6", 6);
                put("7", 7);
                put("8", 8);
            }
        };
        if (codingMap.containsKey(code)) {
            return codingMap.get(code.toLowerCase());
        } else {
            return 0;
        }
    }
}
