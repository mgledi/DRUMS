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
package com.unister.semweb.drums.sync;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.unister.semweb.drums.GlobalParameters;
import com.unister.semweb.drums.bucket.Bucket;
import com.unister.semweb.drums.bucket.DynamicMemoryAllocater;
import com.unister.semweb.drums.storable.AbstractKVStorable;
import com.unister.semweb.drums.synchronizer.ISynchronizerFactory;
import com.unister.semweb.drums.synchronizer.Synchronizer;

/**
 * An instance of a {@link SyncThread}. Synchronizes a {@link Bucket} with the file system.
 * 
 * @author Martin Nettling
 * @param <Data>
 *            an implementation of AbstarctKVStorable
 */
public class SyncThread<Data extends AbstractKVStorable> implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(SyncThread.class);

    /** It is used to share buckets between the {@link SyncManager} and the {@link SyncThread}s. */
    private Bucket<Data> bucket;

    /**
     * this variable is instantiated by the {@link SyncManager} and shows which buckets are actually processed. If the
     * process of the actual <code>bucket</code> is done, its id is removed from this set.
     */
    private Set<Bucket<Data>> actualProcessingBuckets;

    /** A factory to create new synchronizer. */
    private ISynchronizerFactory<Data> synchronizerFactory;

    /** The buffer that created this thread. */
    private SyncManager<Data> buffer;

    /** A Pointer to the GlobalParameters used by the DRUMS containing this SyncThread */
    GlobalParameters<Data> gp;

    /**
     * Constructor. The given {@link Bucket} will be processed.
     * 
     * @param syncManager
     * @param bucket
     *            the {@link Bucket} to synchronize to HDD
     * @param actualProcessingBuckets
     *            pointer to a set of buckets in process
     * @param synchronizerFactory
     *            an implementation of {@link ISynchronizerFactory}. This factory is used for instantiating a
     *            {@link Synchronizer}. The latter is responsible for writing the {@link Bucket} to its corresponding
     *            file to disk
     * @param gp
     */
    public SyncThread(
            SyncManager<Data> syncManager,
            Bucket<Data> bucket,
            Set<Bucket<Data>> actualProcessingBuckets,
            ISynchronizerFactory<Data> synchronizerFactory,
            GlobalParameters<Data> gp) {
        this.gp = gp;
        this.bucket = bucket;
        this.actualProcessingBuckets = actualProcessingBuckets;
        this.synchronizerFactory = synchronizerFactory;
        this.buffer = syncManager;
    }

    /**
     * runs synchronization of a particular {@link Bucket}
     */
    public void run() {
        AbstractKVStorable[] linkData = bucket.getBackend(); // get all LinkData

        long startTime = System.nanoTime(); // remember the, time when synchronizing is started (for logging)

        log.debug("Start to synchronize {} objects.", linkData.length);
        try {
            String filename = buffer.bucketContainer.getHashFunction().getFilename(bucket.getBucketId());
            String directoryName = buffer.getPathToDbFiles();
            Synchronizer<Data> synchronizer = synchronizerFactory
                    .createSynchronizer(directoryName + "/" + filename, gp);
            synchronizer.upsert(linkData); // start synchronizing

            actualProcessingBuckets.remove(bucket);
            freeMemory(bucket);
            synchronizer.close();
            log.debug("Synchronized {} objects in {} ms.", linkData.length, ((System.nanoTime() - startTime) / 1e6));
            /* update messages */
            buffer.sumUpInserted(synchronizer.getNumberOfInsertedEntries());
            buffer.sumUpUpdated(synchronizer.getNumberOfUpdatedEntries());
        } catch (Exception ex) {
            log.error("An error occurred during synchronizing. Synchronizing thread stopped! Some data was lost", ex);
            freeMemory(bucket);
            actualProcessingBuckets.remove(bucket);

        }
    }

    private void freeMemory(Bucket<Data> bucket) {
        log.debug("Try to free memory from bucket {}.", bucket.getBucketId());
        long mem = bucket.freeMemory();
        DynamicMemoryAllocater.INSTANCES[gp.ID].freeMemory(mem);
        log.debug("{} bytes are available now.", mem);

    }
}