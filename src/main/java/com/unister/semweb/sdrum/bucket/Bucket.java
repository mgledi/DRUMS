package com.unister.semweb.sdrum.bucket;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.unister.semweb.sdrum.bucket.hashfunction.AbstractHashFunction;
import com.unister.semweb.sdrum.file.FileLockException;
import com.unister.semweb.sdrum.storable.AbstractKVStorable;
import com.unister.semweb.sdrum.utils.KeyUtils;

/**
 * An instance of this class is a container of {@link AbstractKVStorable}s.
 * 
 * @author m.gleditzsch, n.thieme
 * 
 */
public class Bucket<Data extends AbstractKVStorable<Data>> {

    /**
     * contains all elements to store. We use byte-arrays to save memory, cause mostly the whole object-structure takes
     * double memory.
     */
    private byte[][] backend;

    /** the id of the bucket. Should be known by its {@link BucketContainer} and its {@link AbstractHashFunction} */
    private final int bucketId;

    /** the allowed size of the bucket. For faster access, should be public */
    public final int allowedBucketSize;

    /** the number of elements in this bucket. For faster access, should be public */
    public int elementsInBucket;

    /** prototype of type Data (extending {@link AbstractKVStorable}) for instantiating correct arrays */
    private Data prototype;

    /** Stores the creation time of this bucket. */
    private long creationTime;

    /**
     * Constructor. Needs to know the id of the {@link Bucket} and the maximum size of the {@link Bucket}.
     * 
     * @param bucketId
     *            the identification number of this bucket. Used in {@link BucketContainer} and other classes
     * @param allowedBucketSize
     *            the maximal size of this bucket
     */
    public Bucket(final int bucketId, final int allowedBucketSize, Data prototype) {
        this.bucketId = bucketId;
        this.backend = new byte[allowedBucketSize][];
        this.elementsInBucket = 0;
        this.allowedBucketSize = allowedBucketSize;
        this.prototype = prototype;
        this.creationTime = System.currentTimeMillis();
    }

    /**
     * returns a new empty Bucket with the same properties of this bucket
     * 
     * @throws IOException
     * @throws FileLockException
     */
    public Bucket<Data> getEmptyBucketWithSameProperties() throws FileLockException, IOException {
        Bucket<Data> newBucket = new Bucket<Data>(this.bucketId, this.allowedBucketSize, prototype);
        return newBucket;
    }

    /**
     * Returns the capacity of the Bucket.
     * 
     * @return int
     */
    public int getAllowedBucketSize() {
        return allowedBucketSize;
    }

    /**
     * returns the id of the bucket. The id is in range 0 till numberOfBuckets (see {@link BucketContainer})
     * 
     * @return
     */
    public int getBucketId() {
        return bucketId;
    }

    /**
     * Adds one {@link AbstractKVStorable}-object. This method have to be synchronized, because it is possible to access
     * the <code>backend</code> in the same moment with the function <code>getBackend()</code>. The method returns
     * <code>true</code> if the inseration was successful.
     * 
     * @param toAdd
     *            the {@link AbstractKVStorable} to add
     */
    public synchronized boolean add(AbstractKVStorable<?> toAdd) {
        boolean wasAdded = false;
        if (size() < allowedBucketSize) {
            byte[] b = toAdd.toByteBuffer().array();
            backend[elementsInBucket] = b;
            elementsInBucket++;
            wasAdded = true;
        }
        return wasAdded;
    }

    /** Returns true, of this bucket, contains the given element. */
    public boolean contains(Data element) {
        boolean contains = false;
        for(int i=0; i < elementsInBucket; i++) {
            if(KeyUtils.compareKey(element.toByteBuffer().array(), backend[i]) == 1) {
                contains = true;
                break;
            }
        }
        return contains;
    }
    
    /**
     * Adds a number of {@link AbstractKVStorable}-objects. This method have to be synchronized, because it is possible
     * to access the <code>backend</code> in the same moment with the function <code>getBackend()</code>.
     * 
     * @param toAdd
     *            the {@link AbstractKVStorable}s to add
     */
    // public synchronized void addAll(AbstractKVStorable<?>[] toAdd) {
    // for (AbstractKVStorable<?> oneDate : toAdd) {
    // add(oneDate);
    // }
    // }

    /**
     * Returns the in <code>backend</code> stored {@link AbstractKVStorable}s. First it rebuilds all objects from their
     * byte-arrays and then sorts them.
     * 
     * @return {@link AbstractKVStorable}[] all {@link AbstractKVStorable}s ascending sorted
     */
    public synchronized Data[] getBackend() {
        Data[] data = (Data[]) new AbstractKVStorable[elementsInBucket];
        for (int i = 0; i < elementsInBucket; i++) {
            ByteBuffer bufferData = ByteBuffer.wrap(backend[i]);
            Data reconstructedData = prototype.fromByteBuffer(bufferData);
            data[i] = reconstructedData;
        }

        SortMachine.quickSort(data);
        return (Data[]) data;
    }

    /**
     * returns the number of elements, actual stored
     * 
     * @return int
     */
    public int size() {
        return elementsInBucket;
    }

    /**
     * Returns the creation time of this bucket.
     * 
     * @return
     */
    public long getCreationTime() {
        return creationTime;
    }
}
