package com.unister.semweb.sdrum.bucket.hashfunction;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.unister.semweb.sdrum.file.FileLockException;
import com.unister.semweb.sdrum.file.HeaderIndexFile;
import com.unister.semweb.sdrum.storable.AbstractKVStorable;

/**
 * This abstract class reflects the usage of the hash-function. If the concrete implementation is correct, we just need
 * to know the possible range of the hashFunction (<code>getNumberOfBuckets</code>) and a transformation from a
 * key-value to a bucket-id (<code>getBucketId()</code>).
 * 
 * @author m.gleditzsch
 */
public abstract class AbstractHashFunction {

    public int keySize;

    /** the number of buckets */
    protected int buckets;

    /** returns the number of buckets, adressed by this hashfunction */
    public int getNumberOfBuckets() {
        return buckets;
    }

    /** returns the bucket-id belonging to the given key */
    public abstract int getBucketId(byte[] key);

    public int getBucketId(long key) {
        byte[] b = new byte[8];
        ByteBuffer.wrap(b).putLong(key);
        return getBucketId(b);
    }

    /** returns the bucket-id belonging to the given {@link KVStorable} */
    public abstract int getBucketId(AbstractKVStorable key);

    /** Returns the filename of the bucket with the given bucket. */
    public abstract String getFilename(int bucketId);

    /** Returns the bucketid of the bucket belonging to the given filenam. */
    public abstract int getBucketId(String dbFilename);

    /**
     * This function estimates the optimal bucket size for the given bucket-id from the already stored file. The given
     * threshold shows how many bytes relative to the already store bytes should be hold in memory.
     * 
     * @param threshold
     *            a value between 0 and 1
     * @param bucketId
     *            the id of the bucket
     * @throws IOException
     * @throws FileLockException
     * @return int the size of the bucket
     */
    public static <Data extends AbstractKVStorable> int estimateOptimalBucketSize(AbstractHashFunction hashfunction,
            double threshold, int bucketId)
            throws FileLockException, IOException {
        if (threshold > 1) {
            threshold = 1;
        } else if (threshold < 0) {
            threshold = 0.01;
        }

        String fileName = hashfunction.getFilename(bucketId);
        HeaderIndexFile<Data> file = new HeaderIndexFile<Data>(fileName, 100, null);
        return (int) (file.getFilledUpFromContentStart() / file.getElementSize());
    }

    /**
     * This function estimates the optimal bucket sizes for all buckets from the already stored file. The given
     * threshold shows how many bytes relative to the already store bytes should be hold in memory.
     * 
     * @param threshold
     *            a value between 0 and 1
     * @throws IOException
     * @throws FileLockException
     * @return int[] the sizes of the buckets
     */
    public static <Data extends AbstractKVStorable> int[] estimateOptimalBucketSizes(AbstractHashFunction hashfunction,
            double threshold)
            throws FileLockException, IOException {
        int[] sizes = new int[hashfunction.getNumberOfBuckets()];
        for (int i = 0; i < hashfunction.getNumberOfBuckets(); i++) {
            String fileName = hashfunction.getFilename(i);
            HeaderIndexFile<Data> file = new HeaderIndexFile<Data>(fileName, 100, null);
            sizes[i] = (int) (file.getFilledUpFromContentStart() / file.getElementSize());
        }
        return sizes;
    }
}
