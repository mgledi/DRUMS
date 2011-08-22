package com.unister.semweb.sdrum.api;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.unister.semweb.sdrum.bucket.Bucket;
import com.unister.semweb.sdrum.bucket.BucketContainer;
import com.unister.semweb.sdrum.bucket.BucketContainerException;
import com.unister.semweb.sdrum.bucket.hashfunction.AbstractHashFunction;
import com.unister.semweb.sdrum.file.FileLockException;
import com.unister.semweb.sdrum.file.HeaderIndexFile;
import com.unister.semweb.sdrum.file.IndexForHeaderIndexFile;
import com.unister.semweb.sdrum.storable.AbstractKVStorable;
import com.unister.semweb.sdrum.sync.SyncManager;
import com.unister.semweb.sdrum.synchronizer.SynchronizerFactory;
import com.unister.semweb.sdrum.synchronizer.UpdateOnlySynchronizer;

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
    private static final Logger log = LoggerFactory.getLogger(SDRUM.class);

    /** the accessmode for SDRUM */
    public enum AccessMode {
        READ_ONLY, READ_WRITE;
    }

    /** the number of retries if a file is locked by another process */
    private static final int HEADER_FILE_LOCK_RETRY = 100;

    /** the mode of access. Might be read and write or read only */
    private AccessMode accessMode;

    /** the hashfunction, decides where to search for element, or where to store it */
    private AbstractHashFunction hashFunction;

    /** The directory of the database files. */
    private String databaseDirectory;

    /** the size of the buckets in memory */
    private int sizeOfBuckets;

    /** the size of the pre queue, used if a specific bucket waits for synchronizing */
    private int sizeOfPreQueue;

    /** an array, containing all used buckets */
    private Bucket<Data>[] buckets;

    /** the container for all buckets */
    private BucketContainer<Data> bucketContainer;

    /** the Synchronizer factory, is needed to decide how to insert/update elements */
    private SynchronizerFactory<Data> synchronizerFactory;

    /** the buffer manages the different synchrinize-processes */
    private SyncManager<Data> buffer;

    /** a prototype of the elements to store */
    private Data prototype;

    /** Size of {@link Data} for fast access. */
    private int elementSize;

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
    private SDRUM(String databaseDirectory, int preQueueSize, int numberOfSynchronizerThreads,
            AbstractHashFunction hashFunction, Data prototype, AccessMode accessMode) {
        this.prototype = prototype;
        this.elementSize = prototype.getByteBufferSize();
        this.accessMode = accessMode;
        this.databaseDirectory = databaseDirectory;

        if (accessMode == AccessMode.READ_WRITE) {
            this.sizeOfPreQueue = preQueueSize;
            this.hashFunction = hashFunction;

            buckets = new Bucket[hashFunction.getNumberOfBuckets()];
            for (int i = 0; i < hashFunction.getNumberOfBuckets(); i++) {
                buckets[i] = new Bucket<Data>(i, hashFunction.getBucketSize(i), prototype);
            }
            bucketContainer = new BucketContainer<Data>(buckets, sizeOfPreQueue, hashFunction);
            synchronizerFactory = new SynchronizerFactory<Data>(prototype);
            buffer = new SyncManager<Data>(
                    bucketContainer,
                    numberOfSynchronizerThreads,
                    databaseDirectory,
                    synchronizerFactory);
            buffer.start();
        }

    }

    /**
     * This method creates a new SDRUM object.<br/>
     * If the given directory doesn't exist, it will be created.<br/>
     * If the given directory already exists an {@link IOException} will be thrown.
     * 
     * @param databaseDirectory
     *            the path, where to store the database files
     * @param sizeOfMemoryBuckets
     *            the size of each bucket within memory
     * @param preQueueSize
     *            the size of the prequeue
     * @param numberOfSynchronizerThreads
     *            the number of threads used for synchronizing
     * @param hashFunction
     *            the hash function, decides where to store/search elements
     * @throws IOException
     *             is thrown if an error occurs or if the SDRUM already exists
     * @return new {@link SDRUM}-object
     */
    public static <Data extends AbstractKVStorable<Data>> SDRUM<Data> createTable(String nameOfConfigurationFile,
            String databaseDirectory, int sizeOfMemoryBuckets, int preQueueSize, int numberOfSynchronizerThreads,
            AbstractHashFunction hashFunction, Data prototype) throws IOException {
        File databaseDirectoryFile = new File(databaseDirectory);
        if (databaseDirectoryFile.exists()) {
            throw new IOException("The directory already exist. Can't create a SDRUM.");
        }
        // First we create the directory structure.
        new File(databaseDirectory).mkdirs();

        SDRUM<Data> table = new SDRUM<Data>(databaseDirectory, preQueueSize, numberOfSynchronizerThreads, hashFunction,
                prototype, AccessMode.READ_WRITE);

        // We store the configuration parameters within the given configuration file.
        ConfigurationFile<Data> configurationFile = new ConfigurationFile<Data>(hashFunction.getNumberOfBuckets(),
                sizeOfMemoryBuckets, numberOfSynchronizerThreads, preQueueSize, databaseDirectory, hashFunction,
                prototype);
        configurationFile.writeTo(nameOfConfigurationFile);
        return table;
    }

    /**
     * This method creates a new SDRUM object. The old SDRUM will be overwritten. <br/>
     * If the given directory doesn't exist, it will be created.<br/>
     * A configuration file will be written to the <code>nameOfConfigurationFile</code> location.
     * 
     * @param databaseDirectory
     *            the path, where to store the database files
     * @param sizeOfMemoryBuckets
     *            the size of each bucket within memory
     * @param preQueueSize
     *            the size of the prequeue
     * @param numberOfSynchronizerThreads
     *            the number of threads used for synchronizing
     * @param hashFunction
     *            the hash function, decides where to store/search elements
     * @throws IOException
     *             if an error occurs while writing the configuration file
     * @return new {@link SDRUM}-object
     * @return
     */
    public static <Data extends AbstractKVStorable<Data>> SDRUM<Data> forceCreateTable(String nameOfConfigurationFile,
            String databaseDirectory, int sizeOfMemoryBuckets, int preQueueSize, int numberOfSynchronizerThreads,
            AbstractHashFunction hashFunction, Data prototype) throws IOException {
        File databaseDirectoryFile = new File(databaseDirectory);
        if (databaseDirectoryFile.exists()) {
            deleteDatabaseFilesWithinDirectory(databaseDirectory);
        }

        // We store the configuration parameters within the given configuration file.
        ConfigurationFile<Data> configurationFile = new ConfigurationFile<Data>(hashFunction.getNumberOfBuckets(),
                sizeOfMemoryBuckets, numberOfSynchronizerThreads, preQueueSize, databaseDirectory, hashFunction,
                prototype);
        configurationFile.writeTo(nameOfConfigurationFile);

        SDRUM<Data> table = new SDRUM<Data>(databaseDirectory, preQueueSize, numberOfSynchronizerThreads, hashFunction,
                prototype, AccessMode.READ_WRITE);
        return table;
    }

    /**
     * All database files (extension "*.db") in the given directory will be deleted.
     * 
     * @param databaseDirectory
     *            directory of database files that will be deleted
     */
    private static void deleteDatabaseFilesWithinDirectory(String databaseDirectory) {
        File databaseDirectoryFile = new File(databaseDirectory);
        Collection<File> toDelete = FileUtils.listFiles(databaseDirectoryFile, new String[] { "*.db" }, false);
        // TODO: dont forget to delete the config file
        for (File oneFile : toDelete) {
            oneFile.delete();
        }
    }

    /**
     * Opens an existing table. Only the database configuration file and the access are needed. All other information
     * are loaded from the configuration file.
     * 
     * @param configurationFilename
     *            the name of the configuration file of that SDRUM to open
     * @param accessMode
     *            the AccessMode, how to access the SDRUM
     * @return the table
     */
    public static <Data extends AbstractKVStorable<Data>> SDRUM<Data> openTable(
            String configurationFilename,
            AccessMode accessMode,
            Data prototype)
            throws IOException, ClassNotFoundException {

        ConfigurationFile<Data> configFile = ConfigurationFile.readFrom(configurationFilename);

        if (configFile.getPrototype().getByteBufferSize() != prototype.getByteBufferSize()) {
            log.error("The protoype of the user was different from the prototype stored within the configuration file.");
            log.error("It seems that the prototype has changed over time. To prevent damage at the data the opening process is aborted.");
            log.error("Size of the user prototype: {}, size of the prototype loaded from configuration file: {}",
                    prototype.getByteBufferSize(), configFile.getPrototype().getByteBufferSize());
            throw new IOException(
                    "The prototypes of the configuration file and the user given prototype were different. Consult the log file for more information");
        }

        SDRUM<Data> table = new SDRUM<Data>(
                configFile.getDatabaseDirectory(),
                configFile.getBucketSize(),
                configFile.getNumberOfSynchronizerThreads(),
                configFile.getHashFunction(),
                configFile.getPrototype(),
                accessMode);
        return table;
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
    public void insertOrMerge(Data[] toPersist) throws FileStorageException, InterruptedException {
        try {
            bucketContainer.addToCache(toPersist);
        } catch (BucketContainerException ex) {
            // This exception can theoretically never be thrown because the hash function should map all keys to a
            // bucket.
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
    public void update(Data[] toPersist) throws IOException {
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
            UpdateOnlySynchronizer<Data> synchronizer = new UpdateOnlySynchronizer<Data>(
                    hashFunction.getFilename(entry.index), prototype);
            @SuppressWarnings("unchecked")
            Data[] toUpdate = (Data[]) entry.value.toArray(new AbstractKVStorable[entry.value.size()]);
            synchronizer.upsert(toUpdate);
        }
    }

    /**
     * Takes a list of keys and searches for that in all buckets. This method uses the <code>searchFor(...)</code>
     * method. If you want Data in a more sequential way (faster) try to use <code>read(...)</code> instead. <br>
     * <br>
     * Returns a list of all found data. This can be less than given keys.
     * 
     * @param keys
     *            the keys to be search for
     * @return the to the keys corresponding data
     */
    public List<Data> select(long... keys) throws FileStorageException {
        List<Data> result = new ArrayList<Data>();
        IntObjectOpenHashMap<LongArrayList> bucketKeyMapping = searchForBuckets(keys);
        String filename;
        for (IntObjectCursor<LongArrayList> entry : bucketKeyMapping) {
            filename = databaseDirectory + "/" + hashFunction.getFilename(entry.key);
            HeaderIndexFile<Data> indexFile = null;
            try {
                indexFile = new HeaderIndexFile<Data>(filename, HeaderIndexFile.AccessMode.READ_ONLY,
                        HEADER_FILE_LOCK_RETRY, elementSize);

                LongArrayList keyList = entry.value;
                result.addAll(searchForData(indexFile, keyList.toArray()));
            } catch (FileLockException ex) {
                log.error("Could not access the file {} within {} retries. The file seems to be locked.", filename,
                        HEADER_FILE_LOCK_RETRY);
                throw new FileStorageException(ex);
            } catch (IOException ex) {
                log.error("An exception occurred while trying to get obejects from the file {}.", filename, ex);
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
                HEADER_FILE_LOCK_RETRY, elementSize);

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
    private IntObjectOpenHashMap<LongArrayList> searchForBuckets(long... keys) {
        IntObjectOpenHashMap<LongArrayList> bucketKeyMapping = new IntObjectOpenHashMap<LongArrayList>();
        int bucketId;
        for (long key : keys) {
            bucketId = hashFunction.getBucketId(key);
            if (!bucketKeyMapping.containsKey(bucketId)) {
                bucketKeyMapping.put(bucketId, new LongArrayList());
            }
            bucketKeyMapping.get(bucketId).add(key);
        }
        return bucketKeyMapping;
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
    public List<Data> searchForData(HeaderIndexFile<Data> indexFile, long[] keys) throws IOException {
        Arrays.sort(keys);
        List<Data> result = new ArrayList<Data>();

        IndexForHeaderIndexFile index = indexFile.getIndex(); // Pointer to the Index
        int actualChunkIdx = 0, lastChunkIdx = -1;
        long actualChunkOffset = 0, oldChunkOffset = -1;
        int indexInChunk = 0;
        ByteBuffer workingBuffer = ByteBuffer.allocate(indexFile.getChunkSize());
        byte[] tmpB = new byte[elementSize]; // stores temporarily the bytestream of an object 
        for (long key : keys) {
            // get actual chunkIndex
            actualChunkIdx = index.getChunkId(key);
            actualChunkOffset = index.getStartOffsetOfChunk(actualChunkIdx);

            // if it is the same chunk as in the last step, use the old readbuffer
            if (actualChunkIdx != lastChunkIdx) {
                // if we have read a chunk
                if (oldChunkOffset > -1) {
                    indexFile.write(oldChunkOffset, workingBuffer);
                    indexInChunk = 0;
                }
                // read a new part to the readBuffer
                indexFile.read(actualChunkOffset, workingBuffer);
            }
            // find offset in workingBuffer
            indexInChunk = findElementInReadBuffer(workingBuffer, key, indexInChunk);
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
     * Traverses the given workingBuffer beginning at indexInChunk (this is a byte-offset).
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
    private int findElementInReadBuffer(ByteBuffer workingBuffer, long key, int indexInChunk) {
        workingBuffer.position(indexInChunk);
        int minElement = indexInChunk / elementSize;
        int numberOfEntries = workingBuffer.limit() / elementSize;
        // binary search
        int maxElement = numberOfEntries - 1;
        int midElement;
        long midKey;
        while (minElement <= maxElement) {
            midElement = minElement + (maxElement - minElement) / 2;
            indexInChunk = midElement * elementSize;
            midKey = workingBuffer.getLong(indexInChunk);
            if (key == midKey) {
                return indexInChunk;
            } else if (midKey > key) {
                maxElement = midElement - 1;
            } else {
                minElement = midElement + 1;
            }
        }
        return -1;
    }

    /** Joins all the SDRUM. */
    public void join() throws InterruptedException {
        buffer.join();
    }
    
    /** Closes the SDRUM. */
    public void close() throws InterruptedException {
        buffer.shutdown();
        buffer.join();
    }
}
