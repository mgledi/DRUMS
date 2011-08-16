package com.unister.semweb.sdrum.bucket.hashfunction;

import java.io.Serializable;

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

    /** the number of buckets */
    protected int buckets;

    /** returns the number of buckets, adressed by this hashfunction */
    public int getNumberOfBuckets() {
        return buckets;
    }

    /** returns the bucket-id belonging to the given key */
    public abstract int getBucketId(long key);

    /** returns the bucket-id belonging to the given {@link KVStorable} */
    public abstract int getBucketId(KVStorable<?> key);
    
    /** Returns the filename of the given bucket.*/
    public abstract String getFilename(int bucketId);
}
