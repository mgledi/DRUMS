package com.unister.semweb.sdrum.sync;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.unister.semweb.sdrum.GlobalParameters;
import com.unister.semweb.sdrum.bucket.Bucket;
import com.unister.semweb.sdrum.bucket.DynamicMemoryAllocater;
import com.unister.semweb.sdrum.storable.AbstractKVStorable;
import com.unister.semweb.sdrum.synchronizer.ISynchronizerFactory;
import com.unister.semweb.sdrum.synchronizer.Synchronizer;

/**
 * An instance of a {@link SyncThread}. Synchronizes a {@link Bucket} with the file system.
 * 
 * @author m.gleditzsch
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

    /** A Pointer to the GlobalParameters used by the SDRUM containing this SyncThread */
    GlobalParameters<Data> gp;

    /**
     * Constructor. The given {@link Bucket} will be processed.
     * 
     * @param bucket
     *            the {@link Bucket} to synchronize to HDD
     * @param actualProcessingBucketIds
     *            pointer to a set of actual processed bucket-ids
     * @param synchronizerFactory
     *            a {@link ISynchronizerFactory} for instantiating a {@link Synchronizer}. The latter is responsible for
     *            writing the {@link Bucket} to its corresponding file on HDD
     */
    public SyncThread(
            SyncManager<Data> buffer,
            Bucket<Data> bucket,
            Set<Bucket<Data>> actualProcessingBuckets,
            ISynchronizerFactory<Data> synchronizerFactory,
            GlobalParameters<Data> gp) {
        this.gp = gp;
        this.bucket = bucket;
        this.actualProcessingBuckets = actualProcessingBuckets;
        this.synchronizerFactory = synchronizerFactory;
        this.buffer = buffer;
    }

    /**
     * runs the processing of the {@link Bucket}
     */
    public void run() {
        AbstractKVStorable[] linkData = bucket.getBackend(); // get all LinkData

        long startTime = System.currentTimeMillis(); // remember the, time when synchronizing is started (for logging)

        log.debug("Start to synchronize {} objects.", linkData.length);
        try {
            String filename = buffer.bucketContainer.getHashFunction().getFilename(bucket.getBucketId());
            String directoryName = buffer.getPathToDbFiles();
            Synchronizer<Data> synchronizer = synchronizerFactory
                    .createSynchronizer(directoryName + "/" + filename, gp);
            synchronizer.upsert(linkData); // start synchronizing

            // TODO does this really work???
            actualProcessingBuckets.remove(bucket);
            log.debug("Try to free memory.");
            DynamicMemoryAllocater.INSTANCES[gp.ID].freeMemory(bucket.freeMemory());
            synchronizer.close();
            log.debug("Synchronized {} objects in {} ms.", linkData.length, (System.currentTimeMillis() - startTime));
            /* update messages */
            buffer.sumUpInserted(synchronizer.getNumberOfInsertedEntries());
            buffer.sumUpUpdated(synchronizer.getNumberOfUpdatedEntries());
        } catch (Exception ex) {
            log.error("An error occurred during synchronizing. Synchronizing thread stopped! Some urls were lost", ex);

            // TODO does this really works?
            actualProcessingBuckets.remove(bucket);
        }
    }
}