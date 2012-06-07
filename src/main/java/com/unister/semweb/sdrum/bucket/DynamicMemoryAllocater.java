package com.unister.semweb.sdrum.bucket;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.unister.semweb.sdrum.GlobalParameters;
import com.unister.semweb.sdrum.storable.AbstractKVStorable;

/**
 * This class manages the dynamic memory distribution for the Buckets. It should be used as Singelton. In Java it is
 * easily possible to allocate and free memory directly. Therefore we just remember which bucket got how many bytes to
 * use.
 * 
 * @author m.gleditzsch
 */
// TODO: share the same memory instead of having all its own memory
public class DynamicMemoryAllocater<Data extends AbstractKVStorable> {
    /** the private Logger */
    private Logger log = LoggerFactory.getLogger(this.getClass());

    @SuppressWarnings("rawtypes")
    public static DynamicMemoryAllocater[] INSTANCES = new DynamicMemoryAllocater[0];

    /** The number of already allocated / used bytes */
    private AtomicLong used_bytes;

    /** The number of the maximal allowed bytes, distributed by this class */
    private final long max_allowed_bytes;

    /** The size of one memory chunk. The size is a multiple of the buffersize of a prototype */
    private final int mem_chunksize;

    /**
     * Instantiates a new {@link DynamicMemoryAllocater}. This method is private, cause we want to this class as
     * Singelton.
     * 
     * @param gp
     *            a pointer to the GlobalParameters used by this SDRUM
     */
    private DynamicMemoryAllocater(GlobalParameters<Data> gp) {
        this.used_bytes = new AtomicLong(0);
        this.mem_chunksize = gp.MEMORY_CHUNK - gp.MEMORY_CHUNK % gp.elementSize;
        this.max_allowed_bytes = gp.BUCKET_MEMORY - gp.BUCKET_MEMORY % this.mem_chunksize;
    }

    /**
     * Instantiates the {@link DynamicMemoryAllocater}, only if there is not already an instance
     * 
     * @param gp
     *            a pointer to the GlobalParameters used by this SDRUM
     */
    public static <Data extends AbstractKVStorable> void instantiate(GlobalParameters<Data> gp) {
        if (INSTANCES.length <= gp.ID) {
            // TODO IS THIS REALLY CORRECT???
            INSTANCES = Arrays.copyOf(INSTANCES, gp.ID + 1);
            INSTANCES[gp.ID] = new DynamicMemoryAllocater<Data>(gp);
        }
    }

    /**
     * This method tries to mark memory as allocated. It allocates as much bytes as defined in <code>MEMORY_CHUNK</code>
     * and returns the number of bytes marked as allocated. This method doesn't really allocate memory.
     * 
     * @return int
     * @throws InterruptedException
     */
    public synchronized int allocateNextChunk() throws InterruptedException {
        // if no memory is available, then check every second if now is memory available
        // First try
        // while ((used_bytes.longValue() + (long) mem_chunksize) > max_allowed_bytes) {
        // log.info("Can't allocate memory. {} bytes already allocated. {} bytes allowed.", used_bytes.longValue(),
        // max_allowed_bytes);
        // Thread.sleep(1000);
        // }

        if ((used_bytes.longValue() + (long) mem_chunksize) > max_allowed_bytes) {
            return 0;
        }
        used_bytes.set(used_bytes.longValue() + (long) mem_chunksize);
        return mem_chunksize;
    }

    /**
     * Marks the given amount of memory as free to use.
     * 
     * @param size
     */
    public void freeMemory(long size) {
        used_bytes.set(used_bytes.longValue() - size);
    }

    /** Returns the number of used bytes, allocated by this {@link DynamicMemoryAllocater} */
    public long getUsedMemory() {
        return used_bytes.get();
    }

    /** Returns the maximal allowed bytes to be used by this {@link DynamicMemoryAllocater} */
    public long getMaxMemory() {
        return max_allowed_bytes;
    }

    /** Returns the number of bytes not allocated by the {@link DynamicMemoryAllocater} */
    public long getFreeMemory() {
        return max_allowed_bytes - used_bytes.longValue();
    }
}
