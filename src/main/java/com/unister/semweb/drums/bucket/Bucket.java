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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import com.unister.semweb.drums.GlobalParameters;
import com.unister.semweb.drums.api.DRUMS;
import com.unister.semweb.drums.bucket.hashfunction.AbstractHashFunction;
import com.unister.semweb.drums.file.FileLockException;
import com.unister.semweb.drums.storable.AbstractKVStorable;
import com.unister.semweb.drums.utils.AbstractKVStorableComparator;
import com.unister.semweb.drums.utils.KeyUtils;

/**
 * An instance of this class is a container of {@link AbstractKVStorable}s.
 * 
 * @author Martin Gleditzsch, Nils Thieme
 */
public class Bucket<Data extends AbstractKVStorable> {

    /**
     * contains all memory chunks and its elements to store. We use byte-arrays to save memory, because mostly the whole
     * object-structure takes double memory for small objects.
     */
    private byte[][] memory;

    private int lastChunkIndex = -1;
    private int position_in_chunk = 0;
    private long memorySizeInBytes = 0;

    /** the id of the bucket. Should be known by its {@link BucketContainer} and its {@link AbstractHashFunction} */
    private final int bucketId;

    /** the number of elements in this bucket. For faster access, should be public */
    public int elementsInBucket;

    /** prototype of type Data (extending {@link AbstractKVStorable}) for instantiating correct arrays */
    private Data prototype;

    /** Stores the creation time of this bucket. */
    private long creationTime;

    /** A pointer to the {@link GlobalParameters} used by this {@link DRUMS} */
    private GlobalParameters<Data> gp;

    /**
     * Constructor. Needs to know the id of the {@link Bucket} and the maximum size of the {@link Bucket}.
     * 
     * @param bucketId
     *            the identification number of this bucket. Used in {@link BucketContainer} and other classes
     * @param gp
     *            a pointer to the {@link GlobalParameters}
     */
    public Bucket(final int bucketId, GlobalParameters<Data> gp) {
        this.bucketId = bucketId;
        this.memory = new byte[0][];
        this.elementsInBucket = 0;
        this.prototype = gp.getPrototype();
        this.creationTime = System.currentTimeMillis();
        this.gp = gp;
    }

    /**
     * returns a new empty Bucket with the same properties of this bucket
     * 
     * @throws IOException
     * @throws FileLockException
     */
    public Bucket<Data> getEmptyBucketWithSameProperties() throws FileLockException, IOException {
        Bucket<Data> newBucket = new Bucket<Data>(this.bucketId, gp);
        return newBucket;
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
     *            the Data to add
     */
    public synchronized boolean add(AbstractKVStorable toAdd) {
        if (memorySizeInBytes >= gp.MAX_MEMORY_PER_BUCKET) {
            return false;
        }
        // no memory is available
        try {
            if (lastChunkIndex == -1 || position_in_chunk == memory[lastChunkIndex].length) {
                boolean allocatingSuccessful = enlargeMemory();

                // No more memory was left so we don't add the element and return false.
                if (!allocatingSuccessful) {
                    return false;
                }
            }
        } catch (InterruptedException e) {
            // TODO: error log
            e.printStackTrace();
        }

        byte[] b = toAdd.toByteBuffer().array();
        for (int i = 0; i < b.length; i++, position_in_chunk++) {
            memory[lastChunkIndex][position_in_chunk] = b[i];
        }
        elementsInBucket++;
        return true;
    }

    /* @param element the element to look for
     * 
     * @return true, if this bucket, contains the given element */
    public boolean contains(Data element) {
        boolean contains = false;
        byte[] dst = new byte[gp.elementSize];
        for (int m = 0; m < memory.length; m++) {
            ByteBuffer bb = ByteBuffer.wrap(memory[m]);
            if (m == memory.length - 1) {
                bb.limit(position_in_chunk);
            }
            while (bb.remaining() > 0) {
                bb.get(dst);
                if (KeyUtils.compareKey(element.toByteBuffer().array(), dst) > 0) {
                    contains = true;
                    break;
                }
            }
            if (contains) {
                break;
            }
        }
        return contains;
    }

    /**
     * Enlarges the memory by one chunk. If this was successful <code>true</code> will be returned, otherwise
     * <code>false</code>.
     * 
     * @throws InterruptedException
     */
    private boolean enlargeMemory() throws InterruptedException {
        int bytesAllocated = DynamicMemoryAllocater.INSTANCES[gp.ID].allocateNextChunk();
        if (bytesAllocated > 0) {

            byte[] mem = new byte[bytesAllocated];
            byte[][] new_mem = new byte[memory.length + 1][];
            for (int i = 0; i < memory.length; i++) {
                new_mem[i] = memory[i];
            }
            lastChunkIndex++;
            new_mem[lastChunkIndex] = mem;
            memory = new_mem;
            position_in_chunk = 0;
            return true;
        } else {
            return false;
        }
    }

    private void sort() {

    }

    /**
     * Returns a ByteBuffer, which is pointing to the subarray, where the requested element is stored. The returned
     * ByteBuffer is in read-only mode.
     * 
     * @param index
     * @return
     */
    public ByteBuffer getElementAt(int index) {
        if (index >= elementsInBucket) {
            return null;
        } else {
            int chunk = index * gp.elementSize / memory[0].length;
            int offset = index * gp.elementSize % memory[0].length;
            return ByteBuffer.wrap(memory[chunk], offset, gp.elementSize).asReadOnlyBuffer();
        }
    }

    /**
     * Returns the in <code>backend</code> stored {@link AbstractKVStorable}s. First it rebuilds all objects from their
     * byte-arrays and then sorts them.
     * 
     * @return {@link AbstractKVStorable}[] all {@link AbstractKVStorable}s ascending sorted
     */
    // TODO: handle backend internally as long byte-array
    public synchronized Data[] getBackend() {
        sort();

        AbstractKVStorable[] data = new AbstractKVStorable[elementsInBucket];
        byte[] dst = new byte[gp.elementSize];
        int i = 0;
        for (int m = 0; m < memory.length; m++) {
            ByteBuffer bb = ByteBuffer.wrap(memory[m]);
            if (m == memory.length - 1) {
                bb.limit(position_in_chunk);
            }
            while (bb.remaining() > 0) {
                bb.get(dst);
                data[i++] = prototype.fromByteBuffer(ByteBuffer.wrap(dst));
            }
        }
        Arrays.sort(data, new AbstractKVStorableComparator());
        return (Data[]) data;
    }

    /** @return the number of elements, actually stored */
    public int size() {
        return elementsInBucket;
    }

    /**
     * This method frees the allocated memory.
     * 
     * @return the number of bytes which are available now
     */
    public int freeMemory() {
        int size = 0;
        for (int m = 0; m < memory.length; m++) {
            size += memory[m].length;
            memory[m] = null;
        }
        memory = null;
        return size;
    }

    /** @return the creation time of this bucket. */
    public long getCreationTime() {
        return creationTime;
    }
}
