/* Copyright (C) 2012-2013 Unister GmbH
 * 
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA. */
package com.unister.semweb.drums.bucket.hashfunction;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.io.IOUtils;

import com.unister.semweb.drums.bucket.hashfunction.util.RangeHashSorter;
import com.unister.semweb.drums.storable.AbstractKVStorable;
import com.unister.semweb.drums.utils.ByteArrayComparator;
import com.unister.semweb.drums.utils.KeyUtils;

/**
 * This hashFunction maps an element to a specific range. The ranges are not overlapping. It is not needed, that the
 * ranges are consecutive.
 * 
 * @author Martin Nettling
 */
public class RangeHashFunction extends AbstractHashFunction {
    /** the file where the hashfunction is stored human-readable */
    private String hashFunctionFile;

    /** the key composition. E.g. 2 4 2 8 or char int char long */
    private int keyComposition[];

    private byte[][] maxRangeValues;
    private int[] bucketIds;
    private String[] filenames;


    /**
     * This constructor instantiates a new {@link RangeHashFunction} with the given number of ranges. It tries to size
     * all ranges equally.
     * 
     * @param ranges
     *            the number of ranges
     * @param keySize
     *            the size in bytes of the key
     * @param filename
     *            the filename of the file, where to store the hash-function
     */
    public RangeHashFunction(int ranges, int keySize, String filename) {
        this.hashFunctionFile = filename;
        this.buckets = ranges;
        byte[] max = new byte[keySize], min = new byte[keySize];
        Arrays.fill(max, (byte) -1);
        try {
            this.maxRangeValues = KeyUtils.getMaxValsPerRange(min, max, keySize);
        } catch (Exception e) {
            e.printStackTrace();
        }
        this.filenames = new String[ranges];
        for (int i = 0; i < ranges; i++) {
            filenames[i] = i + ".db";
        }
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
     * @param hashFunctionFilename
     *            the file name of the range hash function
     */
    public RangeHashFunction(byte[][] rangeValues, String[] filenames, String hashFunctionFilename /* TODO: prototype */) {
        this.hashFunctionFile = hashFunctionFilename;
        this.buckets = rangeValues.length;
        this.maxRangeValues = rangeValues;
        this.filenames = filenames;
        this.keyComposition = new int[rangeValues[0].length];
        Arrays.fill(keyComposition, 1);
        sort();
    }

    /** Sorts the max range values corresponding to the file names and the bucket sizes. */
    private void sort() {

        RangeHashSorter sortMachine;
        sortMachine = new RangeHashSorter(maxRangeValues, filenames);
        sortMachine.quickSort();
        generateBucketIds();
    }

    /**
     * This method instantiates a new {@link RangeHashFunction} by the given {@link File}. The File contains some long
     * values, which describe the maximal allowed values for the buckets. The minimal value will be the direct successor
     * of the previous maximal value. Remember: the array will be handled circular.
     * 
     * @param file
     *            the file, which contains the maximal keys
     * @throws IOException
     */
    public RangeHashFunction(File file) throws IOException {
        FileReader fileReader = new FileReader(file);
        initialise(fileReader);
        IOUtils.closeQuietly(fileReader);
        this.hashFunctionFile = file.getAbsolutePath();
    }

    /**
     * Creates the RangeHashFunction with the content of the given {@link Reader}.
     * 
     * @param reader
     * @throws IOException
     */
    public RangeHashFunction(Reader reader) throws IOException {
        initialise(reader);
    }

    /** Initializes the RangeHashFunction with the content of the given {@link Reader}. */
    private void initialise(Reader reader) throws IOException {
        List<String> readData = IOUtils.readLines(reader);

        maxRangeValues = new byte[readData.size() - 1][];
        filenames = new String[readData.size() - 1];

        // analyze header
        String[] header = readData.get(0).split("\t");
        int keySize = 0;
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

        RangeHashSorter sortMachine = new RangeHashSorter(maxRangeValues, filenames);
        sortMachine.quickSort();
        generateBucketIds();

    }

    /**
     * Returns the File, where the HashFunction is stored human-readable
     * 
     * @return File
     */
    public String getHashFunctionFile() {
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

    /**
     * @param bucketId
     * @return the maximal key in the bucket with the given bucketId.
     */
    public byte[] getUpperBound(int bucketId) {
        return maxRangeValues[bucketId];
    }

    /** Determines the bucket id to the given <code>key</code>. */
    @Override
    public int getBucketId(byte[] key) {
        int index = searchBucketIndex(key, 0, maxRangeValues.length - 1);
        return bucketIds[index];
    }

    /**
     * Searches for the given <code>key</code> in {@link #maxRangeValues} and returns the index of the corresponding
     * range. Remember: this may not be the bucketId
     */
    protected int searchBucketIndex(byte[] key, int leftIndex, int rightIndex) {
        if (KeyUtils.compareKey(key, maxRangeValues[rightIndex]) > 0) {
            return 0;
        }
        int idx = Arrays.binarySearch(maxRangeValues, leftIndex, rightIndex, key, new ByteArrayComparator());
        idx = idx < 0 ? -idx - 1 : idx;
        if (idx > rightIndex) {
            return -1;
        } else {
            return idx;
        }
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

    
    @Override
    public String toString() {
        StringBuilder ret = new StringBuilder();
        for (int i = 0; i < maxRangeValues[0].length; i++) {
            ret.append('b').append('\t');
        }
        ret.append("filename").append('\t').append("\n");
        for (int i = 0; i < maxRangeValues.length; i++) {
            String oneCSVLine = makeOneLine(maxRangeValues[i], filenames[i]);
            ret.append(oneCSVLine);
        }
        return ret.toString();
    }
    
    /**
     * Writes the hash function, represented as tuples (range, filename) into the file that is linked with the
     * HashFunction. The content of the file is overwritten.
     * 
     * @throws IOException
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
                newMaxRangeValues[k] = this.getUpperBound(i);
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
     * @return the ranges of this hash function.
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
     * @return a copy of this {@link RangeHashFunction}
     */
    public RangeHashFunction copy() {
        RangeHashFunction clone = new RangeHashFunction(maxRangeValues, filenames, hashFunctionFile);
        return clone;
    }

    /**
     * The header of could contain characters which are not numbers. Some of them can be translated into bytes. E.g.
     * char would be two byte.
     * 
     * @param code
     *            the code to look for
     * @return the size of the given code
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
