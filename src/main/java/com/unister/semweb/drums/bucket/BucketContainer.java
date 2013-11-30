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
package com.unister.semweb.drums.bucket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.unister.semweb.drums.DRUMSParameterSet;
import com.unister.semweb.drums.bucket.hashfunction.AbstractHashFunction;
import com.unister.semweb.drums.storable.AbstractKVStorable;
import com.unister.semweb.drums.storable.GeneralStorable;

/**
 * This class handles {@link AbstractKVStorable}-objects in memory. Depending on the given {@link AbstractHashFunction},
 * the {@link BucketContainer} distributes incoming data to a defined number of {@link Bucket}s. This class is allowed
 * to use the by {@link DRUMSParameterSet#BUCKET_MEMORY} defined amount of memory.
 * 
 * 
 * @author Martin Nettling
 * @param <Data>
 *            an implementation of {@link AbstractKVStorable}, e.g. {@link GeneralStorable}
 */
public class BucketContainer<Data extends AbstractKVStorable> {
    private static final Logger log = LoggerFactory.getLogger(BucketContainer.class);
    /**
     * Array containing all {@link Bucket}s. The index of the bucket in this array should be the bucketId of the related
     * {@link Bucket}.
     */
    protected final Bucket<Data>[] buckets;

    /** the HashFunction to determine the bucket-id */
    protected final AbstractHashFunction hashFunction;

    protected boolean shutDownInitiated = false;

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
     * Add the given records to the {@link Bucket}s, if possible. If all {@link Bucket}s are full the method is
     * blocking.
     * 
     * @param toAdd
     *            the data to add
     * @throws BucketContainerException
     * @throws InterruptedException
     */
    public void addToCache(Data... toAdd) throws BucketContainerException, InterruptedException {
        if (shutDownInitiated) {
            throw new BucketContainerException("Shutdown was already initiated. Could not add the given elements.");
        }
        int throwBucketException = -1;
        for (AbstractKVStorable date : toAdd) {
            int indexOfCache = hashFunction.getBucketId(date.getKey());
            // safety first, check if the bucket exists. If not, try to move on. Throw exception at the end
            if (indexOfCache < buckets.length) {
                Bucket<Data> bucket = buckets[indexOfCache];
                // Blocking process, try to add element
                while (!bucket.add(date)) {
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

    /** @return the number of buckets */
    public int getNumberOfBuckets() {
        return hashFunction.getNumberOfBuckets();
    }

    /**
     * @param bucketId
     * @return the bucket with the given bucketId
     */
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

    /**
     * @param element
     * @return Checks, if an record with the key of the given element is already in memory.
     */
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

    /** This method initializes the shutdown of this BucketContainer. No records are accepted anymore. */
    public void shutdown() {
        this.shutDownInitiated = true;
        log.info("Shutting down the bucket container.");
    }
}
