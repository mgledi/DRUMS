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

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.unister.semweb.drums.GlobalParameters;
import com.unister.semweb.drums.storable.AbstractKVStorable;

/**
 * This class manages the dynamic distribution of memory for the Buckets. It should be used as Singelton. This class
 * doesn't really allocate memory. It just allows to manage virtual memory. An object which wants to be controlled
 * easily asks for memory. This object also must give a report, when it doesn't need this memory anymore.
 * 
 * 
 * @author Martin Nettling
 */
// TODO: share the same memory instead of having all its own memory
public class DynamicMemoryAllocater<Data extends AbstractKVStorable> {
    private static Logger logger = LoggerFactory.getLogger(DynamicMemoryAllocater.class);

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
     *            a pointer to the GlobalParameters used by this DRUMS
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
     *            a pointer to the GlobalParameters used by this DRUMS
     */
    public static <Data extends AbstractKVStorable> void instantiate(GlobalParameters<Data> gp) {
        if (INSTANCES.length <= gp.ID) {
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
    public synchronized int allocateNextChunk() {
        if ((used_bytes.longValue() + (long) mem_chunksize) > max_allowed_bytes) {
            logger.info("No memory left");
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
