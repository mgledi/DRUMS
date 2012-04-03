package com.unister.semweb.sdrum.sync;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.unister.semweb.sdrum.bucket.Bucket;
import com.unister.semweb.sdrum.bucket.BucketContainer;
import com.unister.semweb.sdrum.storable.AbstractKVStorable;
import com.unister.semweb.sdrum.synchronizer.ISynchronizerFactory;
import com.unister.semweb.sdrum.synchronizer.Synchronizer;

/**
 * An instance of a {@link SyncManager} is a thread, that handels the synchronization of {@link Bucket}s with their
 * corresponding parts on HDD. The policy for synchronizing is the following one:<br>
 * <li>get the bucket with the most elements and synchronize it, if there are still resources<br> <li>try to synchronize
 * the next full bucket<br>
 * <br>
 * For synchronizing the {@link SyncManager} instantiates new {@link SyncThread}s (each use special {@link Synchronizer}
 * s to move data from cache to HDD).
 * 
 * @author n.thieme, m.gleditzsch
 */
public class SyncManager<Data extends AbstractKVStorable<Data>> extends Thread {
    private static final Logger log = LoggerFactory.getLogger(SyncManager.class);

    /** at least this * allowedElementsPerBucket Elements have to be in a bucket, before synchronizing the bucket */
    public static final double MINFILL_BEFORE_SYNC = 0.3;

    /** Contains the buckets. */
    public final BucketContainer<Data> bucketContainer;

    /** the path to the files, where to store {@link AbstractKVStorable}s */
    private final String pathToDbFiles;

    /** The factory to use by the {@link SyncThread}. */
    private ISynchronizerFactory<Data> synchronizerFactory;

    /**
     * true, if shutdown is initiated. So all buckets will written to HDD. Also the waiting Elements in
     * {@link BucketContainer}.
     */
    private boolean shutDownInitiated;

    /** A set of bucketIds, which are actual in process */
    private Set<Integer> actualProcessingBucketIds;

    /** The {@link ThreadPoolExecutor} handling all {@link SyncThread}s */
    private ThreadPoolExecutor bufferThreads;

    /**
     * the number of allowed buckets waiting for synchronizing. This number should be always larger, than the real
     * number of threads
     */
    private int allowedBucketsInBuffer;

    /** the number of {@link Bucket}s in {@link BucketContainer} */
    /* needed for fast access */
    private int numberOfBuckets;

    /**
     * max time that a bucket can linger in the bucket container before it will be synchronized to disk. The initial
     * storage time will be infinite.
     */
    private long maxBucketStorageTime;

    /**
     * The number of elements that were inserted by Synchronizers. Have to be threadsafe. Because several
     * {@link Synchronizer}s may update this variable.
     */
    private AtomicLong numberOfElementsInserted;

    /**
     * The number of elements that were updated by Synchronizers. Because several {@link Synchronizer}s may update this
     * variable.
     */
    private AtomicLong numberOfElementsUpdated;

    /**
     * Instantiates a new {@link SyncManager} with an already instantiated {@link BucketContainer}. The
     * <code>numberOfBufferThreads</code> will show the number of allowed {@link SyncThread}s used to synchronize the
     * {@link AbstractKVStorable}s from the {@link Bucket}s in {@link BucketContainer} to the HDD.
     * 
     * @param {@link BucketContainer} bucketContainer
     * @param int numberOfBufferThreads
     * @param String
     *            pathToFiles
     * @param {@link ISynchronizerFactory}
     */
    public SyncManager(BucketContainer<Data> bucketContainer, int numberOfBufferThreads, String pathToFiles,
            ISynchronizerFactory<Data> synchronizerFactory) {
        this.bucketContainer = bucketContainer;
        HashSet<Integer> tmpSet = new HashSet<Integer>();
        this.actualProcessingBucketIds = Collections.synchronizedSet(tmpSet);
        this.pathToDbFiles = pathToFiles;
        this.maxBucketStorageTime = Long.MAX_VALUE;
        this.synchronizerFactory = synchronizerFactory;
        this.allowedBucketsInBuffer = numberOfBufferThreads * 4;
        this.numberOfBuckets = bucketContainer.getNumberOfBuckets();

        // instantiating queue and ThreadPoolExecutor for parallel synchronizing
        BlockingQueue<Runnable> queue = new ArrayBlockingQueue<Runnable>(allowedBucketsInBuffer);
        bufferThreads = new ThreadPoolExecutor(numberOfBufferThreads, numberOfBufferThreads, Integer.MAX_VALUE,
                TimeUnit.DAYS, queue);
        this.setName("SyncManager:FileStorage");

        numberOfElementsInserted = new AtomicLong();
        numberOfElementsUpdated = new AtomicLong();
    }

    public void run() {
        do {
            synchronizeBucketsWithHDD();
            bucketContainer.moveElementsFromWaitingQueue();

            // We wait for 1 milli second. This is not much but reduces the CPU load a lot.
            try {
                Thread.sleep(1);
            } catch (InterruptedException ex) {
                log.info("Sync manager was interrupted.");
                break;
            }
        } while (!shutDownInitiated || !bucketsEmpty());

        // BucketContainer is not receiving new elements, but there can still be waiting elements
        try {
            do {
                bucketContainer.moveElementsFromWaitingQueue();
                synchronizeBucketsWithHDD();
            } while (bucketContainer.getNumberOfWaitingElements() > 0 || !bucketsEmpty());
        } catch (Exception ex) {
            log.error("One exception occurred while synchronizing buckets.", ex);
        }

        bufferThreads.shutdown();
        try {
            bufferThreads.awaitTermination(Integer.MAX_VALUE, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * checks, if the buckets are empty. Returns true only if all buckets are empty.
     * 
     * @return true only if all buckets are empty
     */
    private boolean bucketsEmpty() {
        for (int i = 0; i < numberOfBuckets; i++) {
            if (bucketContainer.buckets[i].elementsInBucket > 0) {
                return false;
            }
        }
        return true;
    }

    /** waits a moment for continuing synchronizing */
    private void sleep() {
        try {
            Thread.sleep(250);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * This method manages the synchronizing-process between {@link BucketContainer} and HDD. It is repeatedly called by
     * <code>run()</code>, till shutdown is initiated.
     */
    private void synchronizeBucketsWithHDD() {
        int synchronizedBuckets = 0;
        // run over all buckets
        for (int i = 0; i < numberOfBuckets; i++) {
            Bucket<Data> oldBucket = bucketContainer.buckets[i];

            // if the bucket is empty, then do nothing
            if (oldBucket.elementsInBucket == 0) {
                synchronizedBuckets++;
                continue;
            }

            long elapsedTime = System.currentTimeMillis() - oldBucket.getCreationTime();
            // if the bucket is full, or the bucket is longer then max bucket storage time within the BucketContainer,
            // or the shutdown was initiated, then try to synchronize the buckets
            // At this point we prevent starvation of one bucket if it not filled for a long period of time.
            if (/*oldBucket.elementsInBucket >= oldBucket.getAllowedBucketSize() ||*/ elapsedTime > maxBucketStorageTime
                    || shutDownInitiated) {
                if (!startNewThread(i)) {
                    sleep();
                }
            }
        }

        if (shutDownInitiated) {
            log.info("{} of {} buckets were synchronized.", synchronizedBuckets, bucketContainer.getNumberOfBuckets());
        }

        // if the queue is not full try to fill it
        if (bufferThreads.getQueue().size() < bufferThreads.getMaximumPoolSize()) {
            int bucketId = getLargestBucketId();
            if (bucketId != -1) {
                Bucket<Data> pointer = bucketContainer.buckets[bucketId];
//                TODO: reactivate with memory dependence
                if (
                        pointer.elementsInBucket >= 100000 &&
                        !startNewThread(bucketId)) {
                    sleep();
                }
            }
        }
    }

    /**
     * Starts a new {@link SyncThread} with the given <code>bucketId</code>. Returns true if the {@link SyncThread} was
     * successful started, false otherwise.<br>
     * <br>
     * The {@link SyncThread} is not started, if the bucket is empty, already in process or does not exist.
     * 
     * @param bucketId
     */
    private boolean startNewThread(int bucketId) {
        // bucket is in process or doesnt exist
        if (actualProcessingBucketIds.contains(bucketId) || bucketId == -1) {
            log.debug(
                    "Can't add BufferThread. Bucket {} does not exist or a bucket with the same id is already in processing.",
                    bucketId);
            return false;
        }
        Bucket<Data> oldBucket = bucketContainer.buckets[bucketId];
        // bucket is empty
        if (oldBucket.elementsInBucket == 0) {
            log.debug("Can't add BufferThread. Bucket {} is empty", bucketId);
            return false;
        }

        // BlockingQueue is full
        if (bufferThreads.getQueue().size() == allowedBucketsInBuffer) {
            log.debug("Can't add BufferThread. Too many threads in queue");
            return false;
        }

        actualProcessingBucketIds.add(bucketId);
        // the old Bucket will be replaced by this new bucket
        try {
            Bucket<Data> newBucket = oldBucket.getEmptyBucketWithSameProperties();
            bucketContainer.setBucket(newBucket);
        } catch (Exception e) {
            e.printStackTrace();
        }

        SyncThread<Data> bufferThread = new SyncThread<Data>(this, oldBucket, actualProcessingBucketIds,
                synchronizerFactory);
        bufferThreads.execute(bufferThread);
        return true;
    }

    /**
     * Returns the id of the largest bucket. (the bucket with most elements)
     * 
     * @return int
     */
    private int getLargestBucketId() {
        int max = 0;
        int rememberId = -1;
        for (int i = 0; i < numberOfBuckets; i++) {
            Bucket<Data> oldBucket = bucketContainer.buckets[i];
            if (oldBucket.elementsInBucket > max) {
                max = oldBucket.elementsInBucket;
                rememberId = oldBucket.getBucketId();
            }
        }
        return rememberId;
    }

    /**
     * overwrites the SynchronizerFactory. Be very careful
     * 
     * @param factory
     */
    public void setSynchronizer(ISynchronizerFactory<Data> factory) {
        this.synchronizerFactory = factory;
    }

    /**
     * Initiates the shutdown. All actual buckets with elements will be processed. Be careful. If someone refills the
     * buckets, the thread won't shut down.
     */
    public void shutdown() {
        this.bucketContainer.shutdown();
        log.info("Shuting down the sync manager");
        shutDownInitiated = true;
    }

    /** Returns the directory of the database files. */
    public String getPathToDbFiles() {
        return pathToDbFiles;
    }

    /**
     * Returns the current maximal time that a bucket can linger within the BucketContainer.
     * 
     * @return
     */
    public long getMaxBucketStorageTime() {
        return maxBucketStorageTime;
    }

    /**
     * Sets the maximal time that a bucket can linger within the BucketContainer without synchronsation to hard drive.
     * 
     * @param maxBucketStorageTime
     */
    public void setMaxBucketStorageTime(long maxBucketStorageTime) {
        this.maxBucketStorageTime = maxBucketStorageTime;
    }

    // #########################################################################################
    // #########################################################################################
    // #########################################################################################
    /* Monitoring methods */

    /** Returns the number of buffer threads that are currently active. */
    public int getNumberOfActiveThreads() {
        return bufferThreads.getActiveCount();
    }

    /** Returns the maximal number of buffer threads that are possible. */
    public int getNumberOfMaximalThreads() {
        return bufferThreads.getMaximumPoolSize();
    }

    /** Adds the specific number to the number of inserted entries. */
    public void sumUpInserted(long toSumUp) {
        numberOfElementsInserted.addAndGet(toSumUp);
    }

    /** Adds the specific number to the number of updated entries. */
    public void sumUpUpdated(long toSumUp) {
        numberOfElementsUpdated.addAndGet(toSumUp);
    }

    /** Returns the overall number of inserted elements to the file storage. */
    public long getNumberOfElementsInserted() {
        return numberOfElementsInserted.get();
    }

    /** Returns the overall number of updated elements of the file storage. */
    public long getNumberOfElementsUpdated() {
        return numberOfElementsUpdated.get();
    }

}
