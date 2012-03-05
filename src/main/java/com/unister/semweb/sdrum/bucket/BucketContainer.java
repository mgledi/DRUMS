package com.unister.semweb.sdrum.bucket;

import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.unister.semweb.sdrum.bucket.hashfunction.AbstractHashFunction;
import com.unister.semweb.sdrum.storable.AbstractKVStorable;

/**
 * This class is responsible for adding {@link AbstractKVStorable}-objects to the correct buckets dependent on its
 * {@link AbstractHashFunction}. It has an additional buffer to store elements, which belong to full buckets.
 * 
 * @author m.gleditzsch
 */
public class BucketContainer<Data extends AbstractKVStorable<Data>> {
    private static final Logger log = LoggerFactory.getLogger(BucketContainer.class);
    /**
     * array containing all {@link Bucket}s. The index of the bucket in this array should also be the bucketId of the
     * {@link Bucket}
     */
    /* public for fast access in buffer */
    public final Bucket<Data>[] buckets;

    /** the queue, containing elements which can not be added to their buckets, because the buckets are full. */
    private final BlockingQueue<Data> waitingElements;

    /** the HashFunction to calculate the bucket-id */
    private final AbstractHashFunction hashFunction;

    private boolean shutDownInitiated = false;

    /**
     * @param buckets
     *            the buckets, where to store the {@link AbstractKVStorable}
     * @param sizeOfPreQueue
     *            a preQueue, if a bucket is full this queue is used as buffer, so that the other buckets can further be
     *            filled with {@link AbstractKVStorable}s
     * @param hashFunction
     *            the {@link AbstractHashFunction} is used to map from the key of a {@link AbstractKVStorable}-object to
     *            the relevant {@link Bucket}
     */
    public BucketContainer(final Bucket<Data>[] buckets, final int sizeOfPreQueue,
            final AbstractHashFunction hashFunction) {
        this.buckets = buckets;
        this.waitingElements = new ArrayBlockingQueue<Data>(sizeOfPreQueue); // TODO: configure
        this.hashFunction = hashFunction;
    }

    /**
     * Moves pending elements from the <code>WaitingQueue</code> to their associated buckets
     * 
     * @throws BucketContainerException
     *             if the {@link Bucket} where to add a {@link AbstractKVStorable} does not exist
     * @throws InterruptedException
     */
    public synchronized void moveElementsFromWaitingQueue() {
        Iterator<Data> tmp = waitingElements.iterator();
        while (tmp.hasNext()) {
            // We catch all exceptions if an error occurs here.
            try {
                Data date = tmp.next();
                if (date != null && this.addToCacheWithoutBlocking(date)) {
                    tmp.remove();
                }
            } catch (Exception ex) {
                log.error("Error while moving elements from waiting queue.", ex);
            }
        }
    }

    /**
     * Handles pending {@link AbstractKVStorable}s in the <code>WaitingQueue</code>. Recalls
     * <code>moveElementsFromWaitingQueue</code> still at least one element can be moved to a {@link Bucket}.
     * 
     * @throws BucketContainerException
     *             if the {@link Bucket} where to add a {@link AbstractKVStorable} does not exist
     * @throws InterruptedException
     */
    public void handleWaitingElements() throws BucketContainerException, InterruptedException {
        while (waitingElements.remainingCapacity() == 0) {
            moveElementsFromWaitingQueue();
            // if the queue is still full, wait a moment
            if (waitingElements.remainingCapacity() == 0) {
                Thread.sleep(250);
            }
        }
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
    public void addToCache(Data... toAdd) throws BucketContainerException, InterruptedException {
        if (shutDownInitiated) {
            throw new BucketContainerException("Shutdown was already initiated. Could not add the given elements.");
        }
        int throwBucketException = -1;
        for (Data linkData : toAdd) {
            int indexOfCache = hashFunction.getBucketId(linkData.key);
            // safety first, check if the bucket exists. If not, try to move on. Throw exception at the end
            if (indexOfCache < buckets.length) {
                Bucket<Data> bucket = buckets[indexOfCache];
                if (!bucket.add(linkData)) {
                    waitingElements.put(linkData);
                }
            } else {
                throwBucketException = indexOfCache;
            }

            // if there are too many elements waiting
            if (waitingElements.remainingCapacity() == 0) {
                handleWaitingElements();
            }
        }

        if (throwBucketException != -1) {
            throw new BucketContainerException("Could not insert at least one LinkData-object. One missing bucket was "
                    + throwBucketException);
        }
    }

    /**
     * This method tries to add a {@link AbstractKVStorable}-object to its corresponding bucket, without any cache.
     * Returns true if this action was successful.
     * 
     * @param linkData
     *            the {@link AbstractKVStorable} object to add
     * @return boolean true, if the operation was successful
     * @throws BucketContainerException
     * @throws InterruptedException
     */
    private boolean addToCacheWithoutBlocking(Data linkData) {
        int indexOfCache = hashFunction.getBucketId(linkData.key);

        if (indexOfCache < buckets.length) {
            Bucket<Data> bucket = buckets[indexOfCache];

            if (bucket.add(linkData)) {
                return true;
            }
        }
        return false;
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

    /** returns the number of waiting elements in waiting-queue */
    public int getNumberOfWaitingElements() {
        return this.waitingElements.size();
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

    /* Monitoring methods */

    /** Gets the number of elements that are waiting in the pre queue. */
    public int getFillLevelPreQueue() {
        return waitingElements.size();
    }

    public void shutdown() {
        this.shutDownInitiated = true;
        log.info("Shutting down the bucket container.");
    }
}
