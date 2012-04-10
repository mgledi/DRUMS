package com.unister.semweb.sdrum.bucket;

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
public class DynamicMemoryAllocater<Data extends AbstractKVStorable<Data>> {
    /** the private Logger */
    private Logger log = LoggerFactory.getLogger(this.getClass());

    @SuppressWarnings("rawtypes")
    public static DynamicMemoryAllocater INSTANCE;

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
     * @param prototype
     */
    private DynamicMemoryAllocater(Data prototype) {
        this.used_bytes = new AtomicLong(0);
        this.mem_chunksize = GlobalParameters.MEMORY_CHUNK -
                GlobalParameters.MEMORY_CHUNK % prototype.getByteBufferSize();
        this.max_allowed_bytes = GlobalParameters.BUCKET_MEMORY - GlobalParameters.BUCKET_MEMORY % this.mem_chunksize;
    }

    /** Instantiates the {@link DynamicMemoryAllocater}, only if there is not already an instance */
    public static <Data extends AbstractKVStorable<Data>> void instantiate(Data prototype) {
        if (INSTANCE == null) {
            DynamicMemoryAllocater.INSTANCE = new DynamicMemoryAllocater<Data>(prototype);
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
        while ((used_bytes.longValue() + (long) mem_chunksize) > max_allowed_bytes) {
            log.info("Can't allocate memory. {} bytes already allocated. {} bytes allowed.", used_bytes.longValue(),
                    max_allowed_bytes);
            Thread.sleep(1000);
        }
        used_bytes.set(used_bytes.longValue() + (long) mem_chunksize);
        return mem_chunksize;
    }

    /**
     * Marks the given amount of memory as free to use.
     * 
     * @param size
     */
    public synchronized void freeMemory(long size) {
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
