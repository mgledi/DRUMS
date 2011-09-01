package com.unister.semweb.sdrum.sync;

import java.io.IOException;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.unister.semweb.sdrum.bucket.Bucket;
import com.unister.semweb.sdrum.storable.AbstractKVStorable;
import com.unister.semweb.sdrum.synchronizer.ISynchronizerFactory;
import com.unister.semweb.sdrum.synchronizer.Synchronizer;

/**
 * An instance of a {@link SyncThread}. Synchronizes a {@link Bucket} with the file system.
 * 
 * @author m.gleditzsch
 */
public class SyncThread<Data extends AbstractKVStorable<Data>> implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(SyncThread.class);

    /** It is used to share buckets between the {@link SyncManager} and the {@link SyncThread}s. */
    private Bucket<Data> bucket;

    /**
     * this variable is instantiated by the {@link SyncManager} and shows which buckets are actually processed. If the
     * process of the actual <code>bucket</code> is done, its id is removed from this set.
     */
    private Set<Integer> actualProcessingBucketIds;

    /** A factory to create new synchronizer. */
    private ISynchronizerFactory<Data> synchronizerFactory;

    /** The buffer that created this thread. */
    private SyncManager<Data> buffer;

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
    public SyncThread(SyncManager<Data> buffer, Bucket<Data> bucket, Set<Integer> actualProcessingBucketIds,
            ISynchronizerFactory<Data> synchronizerFactory) {
        this.bucket = bucket;
        this.actualProcessingBucketIds = actualProcessingBucketIds;
        this.synchronizerFactory = synchronizerFactory;
        this.buffer = buffer;
    }

    /**
     * runs the processing of the {@link Bucket}
     */
    public void run() {
        Data[] linkData = bucket.getBackend(); // get all LinkData
        long startTime = System.currentTimeMillis(); // remember the, time when synchronizing is started (for logging)

        log.debug("Start to synchronize {} objects.", linkData.length);
        try {
            String filename = buffer.bucketContainer.getHashFunction().getFilename(bucket.getBucketId());
            String directoryName = buffer.getPathToDbFiles();
            Synchronizer<Data> synchronizer = synchronizerFactory.createSynchronizer(directoryName + "/" + filename);
            synchronizer.upsert(linkData); // start synchronizing
            actualProcessingBucketIds.remove(bucket.getBucketId());
            synchronizer.close();
            log.debug("Synchronized {} objects in {} ms.", linkData.length, (System.currentTimeMillis() - startTime));
            /* update messages */
            buffer.sumUpInserted(synchronizer.getNumberOfInsertedEntries());
            buffer.sumUpUpdated(synchronizer.getNumberOfUpdatedEntries());
        } catch (IOException ex) {
            log.error("An error occurred during synchronizing. Synchronizing thread stopped! Some urls were lost", ex);
            actualProcessingBucketIds.remove(bucket.getBucketId());
        }
    }
}