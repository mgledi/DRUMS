package com.unister.semweb.sdrum.bucket;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.unister.semweb.sdrum.GlobalParameters;
import com.unister.semweb.sdrum.api.SDRUM;
import com.unister.semweb.sdrum.bucket.hashfunction.AbstractHashFunction;
import com.unister.semweb.sdrum.file.FileLockException;
import com.unister.semweb.sdrum.storable.AbstractKVStorable;
import com.unister.semweb.sdrum.utils.KeyUtils;

/**
 * An instance of this class is a container of {@link AbstractKVStorable}s.
 * 
 * @author m.gleditzsch, n.thieme
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

    /** A pointer to the {@link GlobalParameters} used by this {@link SDRUM} */ 
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
        boolean wasAdded = false;
        if (memorySizeInBytes >= gp.MAX_MEMORY_PER_BUCKET) {
            return wasAdded;
        }
        // no memory is available
        try {
            if (lastChunkIndex == -1) {
                enlargeMemory();
            } else if (position_in_chunk == memory[lastChunkIndex].length) {
                enlargeMemory();
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
        wasAdded = true;
        return wasAdded;
    }

    /** Returns true, of this bucket, contains the given element. */
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
                if (KeyUtils.compareKey(element.toByteBuffer().array(), dst) == 1) {
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
     * enlarges the memory by one chunk
     * 
     * @throws InterruptedException
     */
    private void enlargeMemory() throws InterruptedException {
        byte[] mem = new byte[DynamicMemoryAllocater.INSTANCES[gp.ID].allocateNextChunk()];
        byte[][] new_mem = new byte[memory.length + 1][];
        for (int i = 0; i < memory.length; i++) {
            new_mem[i] = memory[i];
        }
        lastChunkIndex++;
        new_mem[lastChunkIndex] = mem;
        memory = new_mem;
        position_in_chunk = 0;
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
    public synchronized AbstractKVStorable[] getBackend() {
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
        SortMachine.quickSort(data);
        return data;
    }

    /**
     * returns the number of elements, actual stored
     * 
     * @return int
     */
    public int size() {
        return elementsInBucket;
    }

    public int freeMemory() {
        int size = 0;
        for (int m = 0; m < memory.length; m++) {
            size += memory[m].length;
            memory[m] = null;
        }
        memory = null;
        return size;
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
