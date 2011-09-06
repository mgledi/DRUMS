package com.unister.semweb.sdrum.bucket.hashfunction;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

import com.unister.semweb.sdrum.file.FileLockException;
import com.unister.semweb.sdrum.file.HeaderIndexFile;
import com.unister.semweb.sdrum.storable.KVStorable;

/**
 * This abstract class reflects the usage of the hash-function. If the concrete implementation is correct, we just need
 * to know the possible range of the hashFunction (<code>getNumberOfBuckets</code>) and a transformation from a
 * key-value to a bucket-id (<code>getBucketId()</code>).
 * 
 * @author m.gleditzsch
 */
public abstract class AbstractHashFunction implements Serializable {
    private static final long serialVersionUID = -5299645747853624533L;

    public static int INITIAL_BUCKET_SIZE = 1000;

    protected int[] bucketSizes;

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
    public abstract int getBucketId(KVStorable<?> key);

    /** Returns the filename of the bucket with the given bucket. */
    public abstract String getFilename(int bucketId);

    /** Returns the size of the bucket with the given bucket-id. */
    public int getBucketSize(int bucketId) {
        return bucketSizes[bucketId];
    }

    /** Returns the size of the bucket with the given bucket-id. */
    public void setBucketSize(int bucketId, int bucketSize) {
        bucketSizes[bucketId] = bucketSize;
    }

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
    public static int estimateOptimalBucketSize(AbstractHashFunction hashfunction, double threshold, int bucketId)
            throws FileLockException, IOException {
        if (threshold > 1) {
            threshold = 1;
        } else if (threshold < 0) {
            threshold = 0.01;
        }

        String fileName = hashfunction.getFilename(bucketId);
        @SuppressWarnings("rawtypes")
        HeaderIndexFile file = new HeaderIndexFile(fileName, 100);
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
    public static int[] estimateOptimalBucketSizes(AbstractHashFunction hashfunction, double threshold)
            throws FileLockException, IOException {
        int[] sizes = new int[hashfunction.getNumberOfBuckets()];
        for (int i = 0; i < hashfunction.getNumberOfBuckets(); i++) {
            String fileName = hashfunction.getFilename(i);
            @SuppressWarnings("rawtypes")
            HeaderIndexFile file = new HeaderIndexFile(fileName, 100);
            sizes[i] = (int) (file.getFilledUpFromContentStart() / file.getElementSize());
        }
        return sizes;
    }
}
