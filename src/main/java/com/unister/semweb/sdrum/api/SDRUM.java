package com.unister.semweb.sdrum.api;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.unister.semweb.sdrum.GlobalParameters;
import com.unister.semweb.sdrum.bucket.Bucket;
import com.unister.semweb.sdrum.bucket.BucketContainer;
import com.unister.semweb.sdrum.bucket.BucketContainerException;
import com.unister.semweb.sdrum.bucket.DynamicMemoryAllocater;
import com.unister.semweb.sdrum.bucket.SortMachine;
import com.unister.semweb.sdrum.bucket.hashfunction.AbstractHashFunction;
import com.unister.semweb.sdrum.file.FileLockException;
import com.unister.semweb.sdrum.file.HeaderIndexFile;
import com.unister.semweb.sdrum.file.IndexForHeaderIndexFile;
import com.unister.semweb.sdrum.storable.AbstractKVStorable;
import com.unister.semweb.sdrum.sync.SyncManager;
import com.unister.semweb.sdrum.synchronizer.ISynchronizerFactory;
import com.unister.semweb.sdrum.synchronizer.SynchronizerFactory;
import com.unister.semweb.sdrum.synchronizer.UpdateOnlySynchronizer;
import com.unister.semweb.sdrum.utils.KeySearcher;
import com.unister.semweb.sdrum.utils.KeyUtils;

/**
 * This class provides the interface for managing the storage of {@link AbstractKVStorable}s. The name SDRUM is a
 * acronym for sorted disk repository with update management. You can imagine a SDRUM like a partitioned table.<br>
 * <br>
 * Use the following code to create a new SDRUM<br>
 * <br>
 * Use the following code to open an old SDRUM<br>
 * <br>
 * 
 * @author n.thieme, m.gleditzsch
 */
public class SDRUM<Data extends AbstractKVStorable<Data>> {
    private static final Logger log = LoggerFactory.getLogger(SDRUM_API.class);

    /** the accessmode for SDRUM */
    public enum AccessMode {
        READ_ONLY, READ_WRITE;
    }

    /** the number of retries if a file is locked by another process */
    private static final int HEADER_FILE_LOCK_RETRY = 100;

    /** the hashfunction, decides where to search for element, or where to store it */
    private AbstractHashFunction hashFunction;

    /** The directory of the database files. */
    private String databaseDirectory;

    /** an array, containing all used buckets */
    private Bucket<Data>[] buckets;

    /** the container for all buckets */
    private BucketContainer<Data> bucketContainer;

    /** the Synchronizer factory, is needed to decide how to insert/update elements */
    private ISynchronizerFactory<Data> synchronizerFactory;

    /** the buffer manages the different synchronize-processes */
    private SyncManager<Data> syncManager;

    /** a prototype of the elements to store */
    private Data prototype;

    /** Size of {@link Data} for fast access. */
    private int elementSize;

    /** Size of the key of a {@link Data} for fast access. */
    private int keySize;

    /**
     * A private constructor.
     * 
     * @param databaseDirectory
     * @param preQueueSize
     * @param numberOfSynchronizerThreads
     * @param hashFunction
     * @param prototype
     * @param accessMode
     */
    protected SDRUM(
            String databaseDirectory,
            int numberOfSynchronizerThreads,
            AbstractHashFunction hashFunction,
            Data prototype,
            AccessMode accessMode) {
        GlobalParameters.initParameters();
        this.prototype = prototype;
        this.hashFunction = hashFunction;
        DynamicMemoryAllocater.instantiate(prototype);
        GlobalParameters.MIN_ELEMENT_IN_BUCKET_BEFORE_SYNC = 
                (int) ((GlobalParameters.BUCKET_MEMORY - GlobalParameters.BUCKET_MEMORY % GlobalParameters.MEMORY_CHUNK) / 
                hashFunction.getNumberOfBuckets() / prototype.getByteBufferSize() / 2);
        
        this.elementSize = prototype.getByteBufferSize();
        this.keySize = prototype.key.length;
        this.databaseDirectory = databaseDirectory;
        
        if (accessMode == AccessMode.READ_WRITE) {
            buckets = new Bucket[hashFunction.getNumberOfBuckets()];
            for (int i = 0; i < hashFunction.getNumberOfBuckets(); i++) {
                try {
                    buckets[i] = new Bucket<Data>(i, prototype.clone());
                } catch (CloneNotSupportedException ex) {
                    log.error("Prototype doesn't provide clone functionality.", ex);
                    throw new RuntimeException(ex);
                }
            }
            bucketContainer = new BucketContainer<Data>(buckets, hashFunction);
            synchronizerFactory = new SynchronizerFactory<Data>(prototype);
            syncManager = new SyncManager<Data>(
                    bucketContainer,
                    numberOfSynchronizerThreads,
                    databaseDirectory,
                    synchronizerFactory);
            syncManager.start();
        }

    }

    /**
     * Sets the {@link SynchronizerFactory}.
     */
    public void setSynchronizerFactory(ISynchronizerFactory<Data> factory) {
        this.synchronizerFactory = factory;
        this.syncManager.setSynchronizer(factory);
    }

    /**
     * Returns a pointer to the local {@link BucketContainer}
     * 
     * @return
     */
    public BucketContainer<Data> getBucketContainer() {
        return this.bucketContainer;
    }

    /**
     * Returns a pointer to the local {@link SyncManager}
     * 
     * @return {@link SyncManager}
     */
    public SyncManager<Data> getSyncManager() {
        return this.syncManager;
    }

    /**
     * Adds or updates the given data to the file storage. If an error occurs a FileStorageException is thrown. The call
     * possible blocks if all memory buckets are full. If the current thread is interrupted then an
     * {@link InterruptedException} will be thrown.
     * 
     * @param toPersist
     *            data to insert or update
     * @throws FileStorageException
     *             if an error occurs
     * @throws InterruptedException
     *             if the call blocks and the current thread is interrupted
     */
    public void insertOrMerge(Data... toPersist) throws FileStorageException, InterruptedException {
        try {
            bucketContainer.addToCache(toPersist);
        } catch (BucketContainerException ex) {
            // This exception should never be thrown because the hash function should map all keys to a bucket.
            throw new FileStorageException(ex);
        }
    }

    /**
     * This method are for efficient update operations. Be careful ONLY update is provided. If the given array contains
     * element, not stored in the SDRUM they will be not respected.<br>
     * <br>
     * Therefore this method uses the {@link UpdateOnlySynchronizer}, which by itselfs uses the Data's implemented
     * update-function to update elements. If you want to merge objects, use <code>insertOrMerge(...)</code> instead.
     * (this is fairly slower)
     * 
     * @throws IOException
     */
    public void update(Data... toPersist) throws IOException {
        // ############ reorder data
        IntObjectOpenHashMap<ArrayList<Data>> bucketDataMapping = new IntObjectOpenHashMap<ArrayList<Data>>();
        int bucketId;
        for (Data d : toPersist) {
            bucketId = hashFunction.getBucketId(d.key);
            if (!bucketDataMapping.containsKey(bucketId)) {
                bucketDataMapping.put(bucketId, new ArrayList<Data>());
            }
            bucketDataMapping.get(bucketId).add(d);
        }

        for (IntObjectCursor<ArrayList<Data>> entry : bucketDataMapping) {
            try {
                UpdateOnlySynchronizer<Data> synchronizer = new UpdateOnlySynchronizer<Data>(this.databaseDirectory
                        + "/" +
                        hashFunction.getFilename(entry.key), prototype.clone());
                @SuppressWarnings("unchecked")
                Data[] toUpdate = (Data[]) entry.value.toArray(new AbstractKVStorable[entry.value.size()]);
                SortMachine.quickSort(toUpdate);
                synchronizer.upsert(toUpdate);
            } catch (CloneNotSupportedException ex) {
                log.error("Could not clone the prototype for updating entries.", ex);
                throw new RuntimeException(ex);
            }
        }
    }

    /**
     * Takes a list of long-keys and transform them byte[]-keys. Overloads
     * <code>public List<Data> select(byte[]... keys)</code>
     * 
     * @param keys
     * @return
     * @throws FileStorageException
     */
    public List<Data> select(long... keys) throws FileStorageException {
        byte[][] bKeys = KeyUtils.transformToByteArray(keys);
        return this.select(bKeys);
    }

    /**
     * Takes a list of keys and searches for that in all buckets. To speed up the search the size of the read buffer can
     * be specified that will be used to read in the database file. Greater values speed up more. The read buffer is the
     * number of elements of data that should hold in memory.
     * 
     * @param keys
     * @return
     * @throws FileStorageException
     */
    public List<Data> select(byte[]... keys) throws FileStorageException {
        List<Data> result = new ArrayList<Data>();
        IntObjectOpenHashMap<ArrayList<byte[]>> bucketKeyMapping = searchForBuckets(keys);
        String filename;
        for (IntObjectCursor<ArrayList<byte[]>> entry : bucketKeyMapping) {
            filename = databaseDirectory + "/" + hashFunction.getFilename(entry.key);
            HeaderIndexFile<Data> indexFile = null;
            try {
                indexFile = new HeaderIndexFile<Data>(filename, HeaderIndexFile.AccessMode.READ_ONLY,
                        HEADER_FILE_LOCK_RETRY, prototype.key.length, elementSize);

                ArrayList<byte[]> keyList = entry.value;
                result.addAll(searchForData(indexFile, keyList.toArray(new byte[keyList.size()][])));
            } catch (FileLockException ex) {
                log.error("Could not access the file {} within {} retries. The file seems to be locked.", filename,
                        HEADER_FILE_LOCK_RETRY);
                throw new FileStorageException(ex);
            } catch (IOException ex) {
                log.error("An exception occurred while trying to get objects from the file {}.", filename, ex);
                throw new FileStorageException(ex);
            } finally {
                if (indexFile != null) {
                    indexFile.close();
                }
            }
        }
        return result;
    }

    /**
     * Reads <code>numberToRead</code> elements (or less if there are not enough elements) from the bucket with the
     * given <code>bucketId</code> beginning at the element offset.
     * 
     * @param bucketId
     *            the id of the bucket where to read the elements from
     * @param elementOffset
     *            the byte offset, where to start reading
     * @param numberToRead
     *            the number of elements to read
     * @return ArrayList containing the data-objects
     * @throws FileLockException
     * @throws IOException
     */

    public List<Data> read(int bucketId, int elementOffset, int numberToRead) throws FileLockException, IOException {
        String filename = databaseDirectory + "/" + hashFunction.getFilename(bucketId);
        HeaderIndexFile<Data> indexFile = new HeaderIndexFile<Data>(filename, HeaderIndexFile.AccessMode.READ_ONLY,
                HEADER_FILE_LOCK_RETRY, prototype.key.length, elementSize);

        List<Data> result = new ArrayList<Data>();
        // where to start
        long actualOffset = elementOffset * elementSize;

        // get the complete buffer
        ByteBuffer dataBuffer = ByteBuffer.allocate(numberToRead * elementSize);
        indexFile.read(actualOffset, dataBuffer);
        dataBuffer.flip();

        byte[] dataArray = new byte[elementSize];
        while (dataBuffer.position() < dataBuffer.limit()) {
            dataBuffer.get(dataArray);
            result.add((Data) prototype.fromByteBuffer(ByteBuffer.wrap(dataArray)));
        }
        indexFile.close();
        return result;
    }

    /**
     * This method searches for each given key the corresponding bucket. It creates a map that points from the bucket-id
     * to a list of keys in that bucket.
     * 
     * @param keys
     *            the keys to search for
     * @return
     */
    private IntObjectOpenHashMap<ArrayList<byte[]>> searchForBuckets(byte[]... keys) {
        IntObjectOpenHashMap<ArrayList<byte[]>> bucketKeyMapping = new IntObjectOpenHashMap<ArrayList<byte[]>>();
        int bucketId;
        for (byte[] key : keys) {
            bucketId = hashFunction.getBucketId(key);
            if (!bucketKeyMapping.containsKey(bucketId)) {
                bucketKeyMapping.put(bucketId, new ArrayList<byte[]>());
            }
            bucketKeyMapping.get(bucketId).add(key);
        }
        return bucketKeyMapping;
    }

    /**
     * Searches for the {@link AbstractKVStorable}s corresponding the given <code>keys</code> within the given
     * <code>indexFile</code>. This is done by using the {@link IndexForHeaderIndexFile} in the given
     * {@link HeaderIndexFile}. If you want to read a set of consecutive elements, try to use the method
     * <code>read()</code>.<br>
     * <br>
     * WARNING: This method performs a full FileScan and is therfore very ineffective.
     * 
     * @param indexFile
     *            {@link HeaderIndexFile}, where to search for the keys
     * @param elementBuffer
     *            the number of elements to be buffered
     * @param keys
     *            the keys to search for
     * @return Arraylist which contains the found data. Can be less than the number of given keys
     */
    public List<Data> searchForDataLinear(HeaderIndexFile<Data> indexFile, int elementBuffer, byte[]... keys)
            throws IOException {
        SortMachine.quickSort(keys);
        List<Data> result = new ArrayList<Data>();

        long position = 0;
        ByteBuffer buffer = ByteBuffer.allocate(elementBuffer * elementSize);
        while (position < indexFile.getFilledUpFromContentStart()) {
            indexFile.read(position, buffer);
            buffer.flip();

            List<Data> extractedData = extractAndRemoveFoundKey(buffer, keys);
            result.addAll(extractedData);
            position = position + (elementBuffer * elementSize);
            buffer.clear();
        }
        return result;
    }

    /**
     * Searches for records with the given <code>keys</code> within the given buffer.
     * 
     * @param buffer
     * @param keys
     * @return
     */
    private List<Data> extractAndRemoveFoundKey(ByteBuffer buffer, byte[]... keys) {
        List<Data> result = new ArrayList<Data>();

        while (buffer.position() < buffer.limit()) {
            byte[] oneData = new byte[elementSize];
            buffer.get(oneData);
            byte[] readKey = Arrays.copyOfRange(oneData, 0, keySize);
            int indexOfKey = KeySearcher.searchFor(keys, readKey);
            if (indexOfKey >= 0) {
                Data newData = prototype.fromByteBuffer(ByteBuffer.wrap(oneData));
                result.add(newData);
            }
        }
        return result;
    }

    /**
     * Searches for the {@link AbstractKVStorable}s corresponding the given <code>keys</code> within the given
     * <code>indexFile</code>. This is done by using the {@link IndexForHeaderIndexFile} in the given
     * {@link HeaderIndexFile}. If you want to do this in a more sequential way, try to use the method
     * <code>read()</code>
     * 
     * @param indexFile
     *            {@link HeaderIndexFile}, where to search for the keys
     * @param keys
     *            the keys to search for
     * @return Arraylist which contains the found data. Can be less than the number of given keys
     */
    public List<Data> searchForData(HeaderIndexFile<Data> indexFile, byte[]... keys) throws IOException {
        SortMachine.quickSort(keys);
        List<Data> result = new ArrayList<Data>();

        IndexForHeaderIndexFile index = indexFile.getIndex(); // Pointer to the Index
        int actualChunkIdx = 0, lastChunkIdx = -1;
        long actualChunkOffset = 0, oldChunkOffset = -1;
        int indexInChunk = 0;
        ByteBuffer workingBuffer = ByteBuffer.allocate(indexFile.getChunkSize());
        byte[] tmpB = new byte[elementSize]; // stores temporarily the bytestream of an object
        for (byte[] key : keys) {
            // get actual chunkIndex
            actualChunkIdx = index.getChunkId(key);
            actualChunkOffset = index.getStartOffsetOfChunk(actualChunkIdx);

            // if it is the same chunk as in the last step, use the old readbuffer
            if (actualChunkIdx != lastChunkIdx) {
                // if we have read a chunk
                if (oldChunkOffset > -1) {
                    // indexFile.write(oldChunkOffset, workingBuffer);
                    indexFile.read(oldChunkOffset, workingBuffer);
                    indexInChunk = 0;
                }
                // read a new part to the readBuffer
                indexFile.read(actualChunkOffset, workingBuffer);
            }
            // find offset in workingBuffer
            indexInChunk = findElementInReadBuffer(workingBuffer, key, indexInChunk);
            if (indexInChunk == -1) {
                indexInChunk = 0;
                continue;
            }
            // read element from workingBuffer
            workingBuffer.position(indexInChunk);
            workingBuffer.get(tmpB);
            result.add(prototype.fromByteBuffer(ByteBuffer.wrap(tmpB)));
            if (indexInChunk == -1) {
                log.warn("Element with key {} was not found and therefore not updated", key);
                indexInChunk = 0;
            }
            lastChunkIdx = actualChunkIdx; // remember last chunk
            oldChunkOffset = actualChunkOffset; // remember last offset of the last chunk
        }
        return result;
    }

    /**
     * Returns the number of elements in the database.
     * 
     * @return
     * @throws IOException
     * @throws FileLockException
     */
    public long size() throws FileLockException, IOException {
        long size = 0L;
        for (int bucketId = 0; bucketId < hashFunction.getNumberOfBuckets(); bucketId++) {
            HeaderIndexFile<Data> headerIndexFile =
                    new HeaderIndexFile<Data>(
                            databaseDirectory + "/" + hashFunction.getFilename(bucketId),
                            HEADER_FILE_LOCK_RETRY);
            size += headerIndexFile.getFilledUpFromContentStart() / prototype.getByteBufferSize();
            headerIndexFile.close();
        }
        return size;
    }

    /**
     * Searches for the given key in workingBuffer, beginning at the given index. Remember: The records in the
     * workingbuffer have to be ordered ascending.
     * 
     * @param workingBuffer
     *            the ByteBuffer to work on
     * @param key
     *            the key to find
     * @param indexInChunk
     *            the start position of reading the <code>workingBuffer</code>
     * @return the byteOffset where the key was found.<br>
     *         -1 if the key wasn't found
     */
    public int findElementInReadBuffer(ByteBuffer workingBuffer, byte[] key, int indexInChunk) {
        workingBuffer.position(indexInChunk);
        int minElement = indexInChunk / elementSize;
        int numberOfEntries = workingBuffer.limit() / elementSize;

        // binary search
        int maxElement = numberOfEntries - 1;
        int midElement;
        byte comp;
        byte[] tempKey = new byte[keySize];

        while (minElement <= maxElement) {
            midElement = minElement + (maxElement - minElement) / 2;
            indexInChunk = midElement * elementSize;

            workingBuffer.position(indexInChunk);
            workingBuffer.get(tempKey);

            comp = KeyUtils.compareKey(key, tempKey, prototype.key.length);
            if (comp == 0) {
                return indexInChunk;
            } else if (comp < 0) {
                maxElement = midElement - 1;
            } else {
                minElement = midElement + 1;
            }
        }
        return -1;
    }

    /**
     * instantiates a new {@link SDrumIterator} and returns it
     * 
     * @return
     */
    public SDrumIterator<Data> getIterator() {
        return new SDrumIterator<Data>(databaseDirectory, hashFunction, prototype);
    }

    /** Joins all the SDRUM. */
    public void join() throws InterruptedException {
        syncManager.join();
    }

    /** Closes the SDRUM. */
    public void close() throws InterruptedException {
        syncManager.shutdown();
        syncManager.join();
    }

    public int getElementSize() {
        return this.elementSize;
    }

    public AbstractHashFunction getHashFunction() {
        return this.hashFunction;
    }

    public void setHashFunction(AbstractHashFunction hash) {
        this.hashFunction = hash;
    }

    public String getDatabaseDirectory() {
        return this.databaseDirectory;
    }

    public int getElementKeySize() {
        return this.keySize;
    }

    public Data getPrototype() {
        return prototype;
    }
}
