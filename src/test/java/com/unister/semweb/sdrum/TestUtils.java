package com.unister.semweb.sdrum;

import java.io.IOException;
import java.lang.reflect.Array;
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

import com.unister.semweb.sdrum.bucket.Bucket;
import com.unister.semweb.sdrum.bucket.SortMachine;
import com.unister.semweb.sdrum.bucket.hashfunction.AbstractHashFunction;
import com.unister.semweb.sdrum.bucket.hashfunction.FirstBitHashFunction;
import com.unister.semweb.sdrum.file.AbstractHeaderFile.AccessMode;
import com.unister.semweb.sdrum.file.FileLockException;
import com.unister.semweb.sdrum.file.HeaderIndexFile;
import com.unister.semweb.sdrum.storable.DummyKVStorable;
import com.unister.semweb.sdrum.synchronizer.Synchronizer;

public class TestUtils {
    public static Random randomGenerator = new Random(System.currentTimeMillis());

    /**
     * Generates buckets. The size of the buckets, the number of the buckets and the elements that will be generated for
     * one bucket are specified.
     * 
     * @param bucketSize
     * @param numberOfBuckets
     * @param numberOfDataPerBucket
     * @return
     */
    public static Bucket<DummyKVStorable>[] generateBuckets(int bucketSize, int numberOfBuckets,
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
        AbstractHashFunction hashFunction = new FirstBitHashFunction(numberOfBuckets);
        List<Bucket<DummyKVStorable>> result = new ArrayList<Bucket<DummyKVStorable>>();
        for (int i = 0; i < hashFunction.getNumberOfBuckets(); i++) {
            Bucket<DummyKVStorable> newBucket = new Bucket<DummyKVStorable>(i, bucketSize, new DummyKVStorable());
            DummyKVStorable[] bucketLinkData = generateTestdata(numberOfDataPerBucket);
            newBucket.addAll(bucketLinkData);
            result.add(newBucket);
        }
        return result;
    }

    public static DummyKVStorable[] generateTestdata(int numberToGenerate) {
        DummyKVStorable[] result = new DummyKVStorable[numberToGenerate];
        for (int i = 0; i < numberToGenerate; i++) {
            DummyKVStorable oneEntry = new DummyKVStorable();
            oneEntry.setKey(randomGenerator.nextLong());
            oneEntry.setRelevanceScore(randomGenerator.nextDouble());
            oneEntry.setParentCount(randomGenerator.nextInt());
            oneEntry.setTimestamp(randomGenerator.nextLong());
            result[i] = oneEntry;
        }
        Arrays.sort(result);
        return result;
    }

    public static DummyKVStorable[] generateTestdata(int numberToGenerate, long maximumValueForKey,
            long allowedUniqueElements) {
        DummyKVStorable[] result = new DummyKVStorable[numberToGenerate];
        for (int i = 0; i < numberToGenerate; i++) {
            DummyKVStorable oneEntry = new DummyKVStorable();

            double dummyValue = Math.round(((randomGenerator.nextDouble()) * (double) allowedUniqueElements))
                    / (double) allowedUniqueElements;
            long newKey = (long) (dummyValue * maximumValueForKey);

            oneEntry.setKey(newKey);
            oneEntry.setRelevanceScore(randomGenerator.nextDouble());
            //            oneEntry.setParentCount(randomGenerator.nextInt());
            oneEntry.setParentCount(1);
            oneEntry.setTimestamp(randomGenerator.nextLong());
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

    public static DummyKVStorable searchFor(DummyKVStorable[] toSearchIn, long idToSearch) {
        DummyKVStorable result = null;
        for (DummyKVStorable oneDate : toSearchIn) {
            if (oneDate.getLongKey() == idToSearch) {
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
        Synchronizer<DummyKVStorable> sync = new Synchronizer<DummyKVStorable>(dbFileName, new DummyKVStorable());
        sync.upsert(linkDataList);
        sync.close();
    }

    /**
     * this function checks, if the file with the given filename contains exactly the given LinkData-objects.
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
        DummyKVStorable prototype = new DummyKVStorable();
        HeaderIndexFile<DummyKVStorable> dbfile = new HeaderIndexFile<DummyKVStorable>(dbFileName,
                AccessMode.READ_ONLY, 1, prototype.keySize, prototype.byteBufferSize);
        ByteBuffer buffer = ByteBuffer.allocate(prototype.byteBufferSize);
        long offset = 0;
        int k = 0;
        while (offset < dbfile.getFilledUpFromContentStart()) {
            dbfile.read(offset, buffer);
            buffer.flip();
            DummyKVStorable newLinkData = new DummyKVStorable(buffer);
//            System.out.println(newLinkData);
//            System.out.println(linkDataList[k]);
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

    /** Creates <code>numberOfData</code> dummy {@link DummyKVStorable}. */
    public static DummyKVStorable[] createDummyData(int numberOfData) {
        DummyKVStorable[] result = new DummyKVStorable[numberOfData];
        for (int i = 0; i < numberOfData; i++) {
            DummyKVStorable newData = createDummyData(i + 1, i, 1d / i);
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
     */
    public static DummyKVStorable[] createDummyData(int firstKey, int lastKey) {
        DummyKVStorable[] result = new DummyKVStorable[lastKey - firstKey];
        for (int i = firstKey; i < lastKey; i++) {
            DummyKVStorable oneDate = createDummyData(i, i + 1, 1d / i);
            result[i - firstKey] = oneDate;
        }
        return result;
    }

    /** Creates a specific {@link DummyKVStorable} with the given key, parentCount and relevanceScore. */
    public static DummyKVStorable createDummyData(long key, int parentCount, double relevanceScore) {
        DummyKVStorable result = new DummyKVStorable();
        result.setKey(key);
        result.setParentCount(parentCount);
        result.setRelevanceScore(relevanceScore);
        return result;
    }

    /** merges the given two arrays to one */
    public static <T> T[] addAll(T[] o1, T[] o2) {
        T[] all = (T[]) Array.newInstance(o1.getClass().getComponentType(), o1.length + o2.length);
        int k = 0;
        for (T d : o1) {
            all[k] = d;
            k++;
        }
        for (T d : o2) {
            all[k] = d;
            k++;
        }
        return all;
    }
}
