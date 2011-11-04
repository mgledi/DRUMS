package com.unister.semweb.sdrum.bucket.hashfunction;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.unister.semweb.sdrum.bucket.hashfunction.util.RangeHashSorter;
import com.unister.semweb.sdrum.storable.KVStorable;
import com.unister.semweb.sdrum.utils.KeyUtils;

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

    /** the key composition. E.g. 2 4 2 8 or char int char long */
    private int keyComposition[];

    private byte[][] maxRangeValues;
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
    public RangeHashFunction(byte[][] rangeValues, String[] filenames, int[] bucketSizes, File file /* TODO: prototype */) {
        this.hashFunctionFile = file;
        this.buckets = rangeValues.length;
        this.maxRangeValues = rangeValues;
        this.filenames = filenames;
        this.keySize = rangeValues[0].length;
        this.keyComposition = new int[rangeValues[0].length];
        Arrays.fill(keyComposition, 1);

        if (bucketSizes == null) {
            this.bucketSizes = new int[maxRangeValues.length];
            Arrays.fill(this.bucketSizes, INITIAL_BUCKET_SIZE);
        } else {
            this.bucketSizes = bucketSizes;
        }
        sort();

    }

    /** Sorts the max range values corresponding to the file names and the bucket sizes. */
    private void sort() {
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
        List<String> readData = IOUtils.readLines(configFile);

        maxRangeValues = new byte[readData.size() - 1][];
        filenames = new String[readData.size() - 1];
        bucketSizes = new int[readData.size() - 1];

        // analyze header
        String[] header = readData.get(0).split("\t");
        keySize = 0;
        keyComposition = new int[header.length - 2];
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
            int byteOffset = 0;
            for (int k = 0; k < keyComposition.length; k++) {
                long tmp = Long.parseLong(Aline[k]);
                for (int b = 0; b < keyComposition[k]; b++) {
                    maxRangeValues[i][byteOffset] = (byte) tmp;
                    tmp = tmp >> 8;
                    byteOffset++;
                }
            }
            filenames[i] = Aline[keyComposition.length];
            bucketSizes[i] = Integer.parseInt(Aline[keyComposition.length + 1]); // TODO: adapt file
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
    public int getBucketId(KVStorable<?> key) {
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
        bufferedWriter.append("filename").append('\t').append("bucketSize").append("\n");
        for (int i = 0; i < maxRangeValues.length; i++) {
            String oneCSVLine = makeOneLine(maxRangeValues[i], bucketSizes[i], filenames[i]);
            bufferedWriter.append(oneCSVLine);
        }
        bufferedWriter.flush();
        bufferedWriter.close();
    }

    /**
     * Concatenates the given range value and the file name to one string. It is used to write the hash function file.
     */
    private String makeOneLine(byte[] value, int bucketSize, String filename) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < value.length; i++) {
            sb.append(value[i]).append('\t');
        }
        sb.append(filename).append('\t').append(bucketSize).append('\n');
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
    public void replace(int bucketId, byte[][] keysToInsert, int sizeOfNewBuckets) {
        if (bucketId < 0 || bucketId >= maxRangeValues.length) {
            throw new IllegalArgumentException("Invalid bucketId: " + bucketId);
        }
        int numberOfPartitions = keysToInsert.length;
        int newSize = this.getNumberOfBuckets() - 1 + numberOfPartitions;
        byte[][] newMaxRangeValues = new byte[newSize][];
        int[] newBucketSizes = new int[newSize];
        String[] newFileNames = new String[newSize];

        int k = 0;
        for (int i = 0; i < this.getNumberOfBuckets(); i++) {
            if (i != bucketId) {
                newMaxRangeValues[k] = this.getMaxRange(i);
                newBucketSizes[k] = this.getBucketSize(i);
                newFileNames[k] = this.getFilename(i);
                k++;
            }
        }
        for (int i = this.getNumberOfBuckets() - 1; i < newSize; i++) {
            k = i - (this.getNumberOfBuckets() - 1);
            newMaxRangeValues[i] = keysToInsert[k];
            newBucketSizes[i] = sizeOfNewBuckets;
            newFileNames[i] = generateFileName(k, this.getFilename(bucketId));
        }

        this.maxRangeValues = newMaxRangeValues;
        this.filenames = newFileNames;
        this.bucketSizes = newBucketSizes;

        sort();
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
        RangeHashFunction clone = new RangeHashFunction(maxRangeValues, filenames, bucketSizes, hashFunctionFile);
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
