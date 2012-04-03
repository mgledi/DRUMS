package com.unister.semweb.sdrum.bucket;

import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.unister.semweb.sdrum.GlobalParameters;
import com.unister.semweb.sdrum.storable.AbstractKVStorable;

/**
 * This class manages the dynamic memory distribution for the Buckets
 * 
 * @author m.gleditzsch
 */
public class DynamicMemoryAllocater<Data extends AbstractKVStorable<Data>> {
    Logger log = LoggerFactory.getLogger(this.getClass());
    
    @SuppressWarnings("rawtypes")
    public static DynamicMemoryAllocater INSTANCE;
    
    private AtomicLong used_bytes;
    private final long max_allowed_bytes;
    /** The size of one memory chunk. The size is a multiple of the buffersize of a prototype */
    private final int mem_chunksize;

    private DynamicMemoryAllocater(Data prototype) {
        this.used_bytes = new AtomicLong(0);
        this.max_allowed_bytes = GlobalParameters.BUCKET_MEMORY;
        this.mem_chunksize = GlobalParameters.MEMORY_CHUNK -
                GlobalParameters.MEMORY_CHUNK % prototype.getByteBufferSize();
    }

    public static <Data extends AbstractKVStorable<Data>> void instantiate(Data prototype) {
        DynamicMemoryAllocater.INSTANCE = new DynamicMemoryAllocater<Data>(prototype);
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
        while ((used_bytes.longValue() + (long)mem_chunksize) > max_allowed_bytes) {
            log.info("Can't allocate memory. {} bytes already allocated. {} bytes allowed.",used_bytes.longValue() ,max_allowed_bytes );
            Thread.sleep(1000);
        }
        return mem_chunksize;
    }

    
    public synchronized void freeMemory(long size) {
        used_bytes.set(used_bytes.longValue() - size);
    }
}
