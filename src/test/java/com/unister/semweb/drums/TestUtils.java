/*
 * Copyright (C) 2012-2013 Unister GmbH
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */
package com.unister.semweb.drums;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;

import com.unister.semweb.drums.GlobalParameters;
import com.unister.semweb.drums.bucket.Bucket;
import com.unister.semweb.drums.bucket.SortMachine;
import com.unister.semweb.drums.bucket.hashfunction.AbstractHashFunction;
import com.unister.semweb.drums.bucket.hashfunction.RangeHashFunction;
import com.unister.semweb.drums.file.FileLockException;
import com.unister.semweb.drums.file.HeaderIndexFile;
import com.unister.semweb.drums.file.AbstractHeaderFile.AccessMode;
import com.unister.semweb.drums.storable.DummyKVStorable;
import com.unister.semweb.drums.synchronizer.Synchronizer;
import com.unister.semweb.drums.utils.KeyUtils;

public class TestUtils {
    public static Random randomGenerator = new Random(System.currentTimeMillis());
    public static GlobalParameters<DummyKVStorable> gp =
            new GlobalParameters<DummyKVStorable>(DummyKVStorable.getInstance());

    /**
     * Generates buckets. The size of the buckets, the number of the buckets and the elements that will be generated for
     * one bucket are specified.
     * 
     * @param bucketSize
     * @param numberOfBuckets
     * @param numberOfDataPerBucket
     * @return
     */
    public static Bucket<DummyKVStorable>[] generateBuckets(
            int bucketSize,
            int numberOfBuckets,
            int numberOfDataPerBucket) {
        List<Bucket<DummyKVStorable>> bucketList = generateBucketList(bucketSize, numberOfBuckets,
                numberOfDataPerBucket);
        Bucket<DummyKVStorable>[] result = new Bucket[bucketList.size()];
        result = bucketList.toArray(result);
        return result;
    }

    /**
     * Generates list of buckets. The list has at least <code>numberOfBuckets</code> buckets but more are possible. The
     * number of buckets points to a multiply of 2. For example: if the given number of buckets is 1000, you will get
     * 1024 buckets.
     * 
     * @param bucketSize
     *            the size of the bucket that will initialise the bucket
     * @param numberOfBuckets
     *            at least numberOfBuckets buckets, but it is always a multiply of 2
     * @param numberOfDataPerBucket
     *            number of generated entries per bucket
     */
    public static List<Bucket<DummyKVStorable>> generateBucketList(int bucketSize, int numberOfBuckets,
            int numberOfDataPerBucket) {
        AbstractHashFunction hashFunction = new RangeHashFunction(numberOfBuckets, gp.keySize,"");
        List<Bucket<DummyKVStorable>> result = new ArrayList<Bucket<DummyKVStorable>>();
        for (int i = 0; i < hashFunction.getNumberOfBuckets(); i++) {
            Bucket<DummyKVStorable> newBucket = new Bucket<DummyKVStorable>(i, TestUtils.gp);
            DummyKVStorable[] bucketLinkData = generateTestdata(numberOfDataPerBucket);
            for (DummyKVStorable oneToAdd : bucketLinkData) {
                newBucket.add(oneToAdd);
            }
            result.add(newBucket);
        }
        return result;
    }

    public static DummyKVStorable[] generateTestdata(int numberToGenerate) {
        DummyKVStorable[] result = new DummyKVStorable[numberToGenerate];
        for (int i = 0; i < numberToGenerate; i++) {
            DummyKVStorable oneEntry = TestUtils.createDummyData(KeyUtils.convert(i + 1), i + 1000, i - 0.05);
            result[i] = oneEntry;
        }
        SortMachine.quickSort(result);
        return result;
    }

    /**
     * Generates the specified number of test data with the specified distance. The distance marks the difference
     * between two keys.
     * 
     * @param numberToGenerate
     * @param distance
     * @return
     */
    public static DummyKVStorable[] generateTestdata(int numberToGenerate, int distance) {
        DummyKVStorable[] result = new DummyKVStorable[numberToGenerate];
        for (int i = 0; i < numberToGenerate; i++) {

            DummyKVStorable oneEntry = TestUtils.createDummyData(
                    KeyUtils.convert((i * distance) + 1),
                    i + 1000,
                    i - 0.05);
            result[i] = oneEntry;
        }
        SortMachine.quickSort(result);
        return result;
    }

    public static DummyKVStorable[] generateTestdata(int numberToGenerate, long maximumValueForKey,
            long allowedUniqueElements) {
        DummyKVStorable[] result = new DummyKVStorable[numberToGenerate];
        for (int i = 0; i < numberToGenerate; i++) {

            double dummyValue = Math.round(((randomGenerator.nextDouble()) * (double) allowedUniqueElements))
                    / (double) allowedUniqueElements;
            long newKey = (long) (dummyValue * maximumValueForKey);

            DummyKVStorable oneEntry = TestUtils.createDummyData(KeyUtils.convert(newKey), 1,
                    randomGenerator.nextDouble());
            result[i] = oneEntry;
        }
        Arrays.sort(result);
        return result;
    }

    public static DummyKVStorable[] generateTestdataDifferentFrom(BlockingQueue<DummyKVStorable> notToInclude,
            int numberOfNewTestdata) {
        List<DummyKVStorable> notIncludeList = new ArrayList<DummyKVStorable>();
        notIncludeList.addAll(notToInclude);

        DummyKVStorable[] result = generateTestdataDifferentFromList(notIncludeList, numberOfNewTestdata);
        return result;
    }

    public static DummyKVStorable[] generateTestdataDifferentFrom(DummyKVStorable[] notToInclude,
            int numberOfNewTestdata) {
        List<DummyKVStorable> notIncludeList = Arrays.asList(notToInclude);

        DummyKVStorable[] result = generateTestdataDifferentFromList(notIncludeList, numberOfNewTestdata);
        return result;
    }

    public static DummyKVStorable[] generateTestdataDifferentFromList(List<DummyKVStorable> notToInclude,
            int numberOfNewTestdata) {
        Collection<DummyKVStorable> subtraction = new ArrayList<DummyKVStorable>();

        while (subtraction.size() != numberOfNewTestdata) {
            DummyKVStorable[] newTestdata = generateTestdata(numberOfNewTestdata - subtraction.size());
            Collections.addAll(subtraction, newTestdata);
            subtraction = CollectionUtils.subtract(subtraction, notToInclude);
        }

        DummyKVStorable[] result = new DummyKVStorable[numberOfNewTestdata];
        result = subtraction.toArray(result);
        return result;
    }

    public static boolean areEqual(DummyKVStorable[] first, DummyKVStorable[] second) {
        if (first.length != second.length)
            return false;
        for (int i = 0; i < first.length; i++) {
            if (!first[i].equals(second[i])) {
                System.out.println("First entry: " + first[i]);
                System.out.println("Second entry: " + second[i]);
                return false;
            }
        }
        return true;
    }

    public static DummyKVStorable[] subtract(DummyKVStorable[] first, DummyKVStorable[] second) {
        List<DummyKVStorable> result = new ArrayList<DummyKVStorable>();
        for (DummyKVStorable oneEntry : first) {
            if (!ArrayUtils.contains(second, oneEntry)) {
                result.add(oneEntry);
            }
        }

        DummyKVStorable[] resultArray = new DummyKVStorable[result.size()];
        return result.toArray(resultArray);
    }

    public static DummyKVStorable searchFor(DummyKVStorable[] toSearchIn, byte[] idToSearch) {
        DummyKVStorable result = null;
        for (DummyKVStorable oneDate : toSearchIn) {
            if (KeyUtils.compareKey(oneDate.key, idToSearch) == 0) {
                result = oneDate;
                break;
            }
        }
        return result;
    }

    /**
     * creates a file with the given filename and fills it with the given data
     * 
     * @param dbFileName
     *            the name of the file
     * @param linkDataList
     *            the array, containing LinkData
     * @throws IOException
     */
    public static void createFile(String dbFileName, DummyKVStorable[] linkDataList) throws IOException {
        SortMachine.quickSort(linkDataList);
        Synchronizer<DummyKVStorable> sync = new Synchronizer<DummyKVStorable>(dbFileName, TestUtils.gp);
        sync.upsert(linkDataList);
        sync.close();
    }

    /**
     * This function checks, if the file with the given filename contains exactly the given LinkData-objects.
     * 
     * @param dbFileName
     *            the name of the file
     * @param linkDataList
     *            the array, containing LinkData
     * @throws IOException
     * @throws FileLockException
     */
    public static boolean checkContentFile(String dbFileName, DummyKVStorable[] linkDataList) throws IOException,
            FileLockException {
        // load file
        DummyKVStorable prototype = gp.getPrototype();
        HeaderIndexFile<DummyKVStorable> dbfile = new HeaderIndexFile<DummyKVStorable>(dbFileName, 1, TestUtils.gp);
        ByteBuffer buffer = ByteBuffer.allocate(prototype.getByteBufferSize());
        long offset = 0;
        int k = 0;
        while (offset < dbfile.getFilledUpFromContentStart()) {
            dbfile.read(offset, buffer);
            buffer.flip();
            DummyKVStorable newLinkData = (DummyKVStorable) prototype.fromByteBuffer(buffer);
            if (!newLinkData.equals(linkDataList[k])) {
                return false;
            }
            k++;
            offset += buffer.limit();
            buffer.clear();
        }
        dbfile.close();
        return true;
    }

    /**
     * Creates <code>numberOfData</code> dummy {@link DummyKVStorable}.
     * 
     * @throws IOException
     */
    public static DummyKVStorable[] createDummyData(int numberOfData) throws IOException {
        DummyKVStorable[] result = new DummyKVStorable[numberOfData];
        for (int i = 0; i < numberOfData; i++) {
            DummyKVStorable newData = createDummyData(KeyUtils.convert(i + 1), i, 1d / i);
            result[i] = newData;
        }
        return result;
    }

    /**
     * Creates a set of {@link DummyKVStorable}. There are (lastKey - firstKey) {@link DummyKVStorable} be generated.
     * The first {@link DummyKVStorable} has as key the <code>firstKey</code>, the last {@link DummyKVStorable} the
     * <code>lastKey</code>.
     * 
     * @param firstKey
     * @param lastKey
     * @return
     * @throws IOException
     */
    public static DummyKVStorable[] createDummyData(int firstKey, int lastKey) {
        DummyKVStorable[] result = new DummyKVStorable[lastKey - firstKey];
        for (int i = firstKey; i < lastKey; i++) {
            DummyKVStorable oneDate = createDummyData(KeyUtils.convert(i), i + 1, 1d / i);
            result[i - firstKey] = oneDate;
        }
        return result;
    }

    /**
     * Creates a specific {@link DummyKVStorable} with the given key, parentCount and relevanceScore.
     * 
     * @throws IOException
     */
    public static DummyKVStorable createDummyData(byte[] key, int parentCount, double relevanceScore) {
        DummyKVStorable kv = DummyKVStorable.getInstance();
        kv.setKey(key);
        try {
            kv.setValue("parentCount", parentCount);
            kv.setValue("relevanceScore", relevanceScore);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return kv;
    }

    /**
     * Reads from the given numbe of elements (<code>numberOfElementsToRead</code>) from the given file from the
     * beginning.
     */
    public static List<DummyKVStorable> readFrom(String filename, int numberOfElementsToRead) throws Exception {
        HeaderIndexFile<DummyKVStorable> file = new HeaderIndexFile<DummyKVStorable>(filename, AccessMode.READ_ONLY, 1,
                TestUtils.gp);
        ByteBuffer dataBuffer = ByteBuffer.allocate(numberOfElementsToRead * TestUtils.gp.elementSize);
        file.read(0, dataBuffer);
        dataBuffer.flip();

        List<DummyKVStorable> readData = new ArrayList<DummyKVStorable>();
        while (dataBuffer.position() < dataBuffer.limit()) {
            byte[] oneLinkData = new byte[TestUtils.gp.elementSize];
            dataBuffer.get(oneLinkData);
            DummyKVStorable oneDate = TestUtils.gp.getPrototype().fromByteBuffer(ByteBuffer.wrap(oneLinkData));
            readData.add(oneDate);
        }
        file.close();
        return readData;
    }

    /** merges the given two arrays to one */
    public static DummyKVStorable[] merge(DummyKVStorable[]... arrays) {
        int size = 0;
        for (DummyKVStorable[] A : arrays)
            size += A.length;

        DummyKVStorable[] all = new DummyKVStorable[size];
        int k = 0;
        for (DummyKVStorable[] A : arrays) {
            for (DummyKVStorable d : A) {
                all[k++] = d;
            }
        }
        return all;
    }
}
