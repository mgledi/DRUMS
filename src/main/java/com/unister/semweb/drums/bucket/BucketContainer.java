package com.unister.semweb.drums.bucket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.unister.semweb.drums.bucket.hashfunction.AbstractHashFunction;
import com.unister.semweb.drums.storable.AbstractKVStorable;

/**
 * This class is responsible for adding {@link AbstractKVStorable}-objects to the correct buckets dependent on its
 * {@link AbstractHashFunction}. It has an additional buffer to store elements, which belong to full buckets.
 * 
 * @author m.gleditzsch
 */
public class BucketContainer<Data extends AbstractKVStorable> {
    private static final Logger log = LoggerFactory.getLogger(BucketContainer.class);
    /**
     * array containing all {@link Bucket}s. The index of the bucket in this array should also be the bucketId of the
     * {@link Bucket}
     */
    /* public for fast access in buffer */
    public final Bucket<Data>[] buckets;

    /** the HashFunction to calculate the bucket-id */
    private final AbstractHashFunction hashFunction;

    private boolean shutDownInitiated = false;

    /**
     * @param buckets
     *            the buckets, where to store the {@link AbstractKVStorable}
     * @param hashFunction
     *            the {@link AbstractHashFunction} is used to map from the key of a {@link AbstractKVStorable}-object to
     *            the relevant {@link Bucket}
     */
    public BucketContainer(final Bucket<Data>[] buckets, final AbstractHashFunction hashFunction) {
        this.buckets = buckets;
        this.hashFunction = hashFunction;
    }

    /**
     * Possibly add the given <code>Data</code>(s) to one of the {@link Bucket}s. If the corresponding {@link Bucket} is
     * full, it will be added to <code>waitingElements</code>. If all {@link Bucket}s and the queue for
     * <code>waitingElements</code> are full the method is blocking.
     * 
     * @param {@link Data} to add
     * @return true if the insertion was successful, otherwise false
     * @throws BucketContainerException
     */
    public void addToCache(Data ... toAdd) throws BucketContainerException, InterruptedException {
        if (shutDownInitiated) {
            throw new BucketContainerException("Shutdown was already initiated. Could not add the given elements.");
        }
        int throwBucketException = -1;
        for (AbstractKVStorable date : toAdd) {
            int indexOfCache = hashFunction.getBucketId(date.key);
            // safety first, check if the bucket exists. If not, try to move on. Throw exception at the end
            if (indexOfCache < buckets.length) {
                Bucket<Data> bucket = buckets[indexOfCache];
                // Blocking process, try to add element
                while(!bucket.add(date)) {
                    Thread.sleep(1000);
                }
            } else {
                throwBucketException = indexOfCache;
            }
        }

        if (throwBucketException != -1) {
            throw new BucketContainerException("Could not insert at least one LinkData-object. One missing bucket was "
                    + throwBucketException);
        }
    }

    /** returns the number of buckets */
    public int getNumberOfBuckets() {
        return hashFunction.getNumberOfBuckets();
    }

    /** returns the bucket with the id bucketId */
    public Bucket<Data> getBucket(int bucketId) {
        return buckets[bucketId];
    }

    /**
     * sets a new bucket. The bucketId is read from the given {@link Bucket}-object
     * 
     * @param bucket
     *            the {@link Bucket}-object to add
     * @throws BucketContainerException
     * @throws InterruptedException
     */
    public void setBucket(Bucket<Data> bucket) throws BucketContainerException, InterruptedException {
        buckets[bucket.getBucketId()] = bucket;
    }

    /** Checks, if the given element is already in memory. */
    public boolean contains(Data element) {
        return buckets[hashFunction.getBucketId(element)].contains(element);        
    }
    
    /**
     * returns the {@link AbstractHashFunction} for mapping keys to {@link Bucket}s.
     * 
     * @return {@link AbstractHashFunction}
     */
    public AbstractHashFunction getHashFunction() {
        return this.hashFunction;
    }

    public void shutdown() {
        this.shutDownInitiated = true;
        log.info("Shutting down the bucket container.");
    }
}
