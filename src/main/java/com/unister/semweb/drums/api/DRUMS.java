/*
 * Copyright (C) 2012-2013 Unister GmbH
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */
package com.unister.semweb.drums.api;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.unister.semweb.drums.GlobalParameters;
import com.unister.semweb.drums.bucket.Bucket;
import com.unister.semweb.drums.bucket.BucketContainer;
import com.unister.semweb.drums.bucket.BucketContainerException;
import com.unister.semweb.drums.bucket.DynamicMemoryAllocater;
import com.unister.semweb.drums.bucket.SortMachine;
import com.unister.semweb.drums.bucket.hashfunction.AbstractHashFunction;
import com.unister.semweb.drums.file.FileLockException;
import com.unister.semweb.drums.file.HeaderIndexFile;
import com.unister.semweb.drums.file.IndexForHeaderIndexFile;
import com.unister.semweb.drums.storable.AbstractKVStorable;
import com.unister.semweb.drums.sync.SyncManager;
import com.unister.semweb.drums.synchronizer.ISynchronizerFactory;
import com.unister.semweb.drums.synchronizer.SynchronizerFactory;
import com.unister.semweb.drums.synchronizer.UpdateOnlySynchronizer;
import com.unister.semweb.drums.utils.KeyUtils;

/**
 * This class provides the interface for managing the storage of {@link AbstractKVStorable}s. The name DRUMS is a
 * acronym for sorted disk repository with update management. You can imagine a DRUMS like a partitioned table.<br>
 * <br>
 * Use the following code to create a new DRUMS<br>
 * <br>
 * Use the following code to open an old DRUMS<br>
 * <br>
 * 
 * @author n.thieme, m.gleditzsch
 */
public class DRUMS<Data extends AbstractKVStorable> {
    private static final Logger logger = LoggerFactory.getLogger(DRUMS_API.class);

    /** the accessmode for DRUMS */
    public enum AccessMode {
        READ_ONLY, READ_WRITE;
    }

    /** the number of retries if a file is locked by another process */
    private static final int HEADER_FILE_LOCK_RETRY = 100;

    /** the hashfunction, decides where to search for element, or where to store it */
    private AbstractHashFunction hashFunction;

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

    /** A pointer to the {@link GlobalParameters} used by this {@link DRUMS} */
    protected GlobalParameters<Data> gp;

    protected DRUMS_Reader<Data> reader_instance;

    /**
     * A private constructor.
     * 
     * @param databaseDirectory
     * @param hashFunction
     * @param prototype
     * @param accessMode
     */
    protected DRUMS(AbstractHashFunction hashFunction, AccessMode accessMode, GlobalParameters<Data> gp) {
        this.prototype = gp.getPrototype();
        this.hashFunction = hashFunction;
        this.gp = gp;
        DynamicMemoryAllocater.instantiate(gp);
        gp.MIN_ELEMENT_IN_BUCKET_BEFORE_SYNC = (int) ((gp.BUCKET_MEMORY - gp.BUCKET_MEMORY % gp.MEMORY_CHUNK)
                / hashFunction.getNumberOfBuckets() / prototype.getByteBufferSize() / 2);
        logger.info("Setted MIN_ELEMENT_IN_BUCKET_BEFORE_SYNC to {}", gp.MIN_ELEMENT_IN_BUCKET_BEFORE_SYNC);
        if (accessMode == AccessMode.READ_WRITE) {
            buckets = new Bucket[hashFunction.getNumberOfBuckets()];
            for (int i = 0; i < hashFunction.getNumberOfBuckets(); i++) {
                buckets[i] = new Bucket<Data>(i, gp);
                String tmpFileName = gp.databaseDirectory + "/" + hashFunction.getFilename(i);
                if (!new File(tmpFileName).exists()) {
                    HeaderIndexFile<Data> tmpFile;
                    try {
                        tmpFile = new HeaderIndexFile<Data>(tmpFileName, HeaderIndexFile.AccessMode.READ_WRITE, 1, gp);
                        tmpFile.close();
                    } catch (FileLockException e) {
                        logger.error("Can't create file {}, because file is locked by another process.", tmpFileName);
                    } catch (IOException e) {
                        logger.error("Can't create file {}. {}", tmpFileName, e);
                    }
                }
            }
            bucketContainer = new BucketContainer<Data>(buckets, hashFunction);
            synchronizerFactory = new SynchronizerFactory<Data>();
            syncManager = new SyncManager<Data>(bucketContainer, synchronizerFactory, gp);
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
     * This method are for efficient update operations. Be careful, ONLY updates are provided. If the given array
     * contains elements, not already stored in the DRUMS, they will be not respected.<br>
     * <br>
     * This method uses the {@link UpdateOnlySynchronizer}, which by itselfs uses the Data's implemented update-function
     * to update elements. If you want to merge objects, use <code>insertOrMerge(...)</code> instead. (this is fairly
     * slower)
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
            UpdateOnlySynchronizer<Data> synchronizer = new UpdateOnlySynchronizer<Data>(gp.databaseDirectory + "/"
                    + hashFunction.getFilename(entry.key), gp);
            @SuppressWarnings("unchecked")
            Data[] toUpdate = (Data[]) entry.value.toArray(new AbstractKVStorable[entry.value.size()]);
            SortMachine.quickSort(toUpdate);
            synchronizer.upsert(toUpdate);
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
        IntObjectOpenHashMap<ArrayList<byte[]>> bucketKeyMapping = getBucketKeyMapping(keys);
        String filename;
        for (IntObjectCursor<ArrayList<byte[]>> entry : bucketKeyMapping) {
            filename = gp.databaseDirectory + "/" + hashFunction.getFilename(entry.key);
            HeaderIndexFile<Data> indexFile = null;
            try {
                indexFile = new HeaderIndexFile<Data>(filename, HeaderIndexFile.AccessMode.READ_ONLY,
                        HEADER_FILE_LOCK_RETRY, gp);

                ArrayList<byte[]> keyList = entry.value;
                result.addAll(searchForData(indexFile, keyList.toArray(new byte[keyList.size()][])));
            } catch (FileLockException ex) {
                logger.error("Could not access the file {} within {} retries. The file seems to be locked.", filename,
                        HEADER_FILE_LOCK_RETRY);
                throw new FileStorageException(ex);
            } catch (IOException ex) {
                logger.error("An exception occurred while trying to get objects from the file {}.", filename, ex);
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
        String filename = gp.databaseDirectory + "/" + hashFunction.getFilename(bucketId);
        HeaderIndexFile<Data> indexFile = new HeaderIndexFile<Data>(filename, HeaderIndexFile.AccessMode.READ_ONLY,
                HEADER_FILE_LOCK_RETRY, gp);

        List<Data> result = new ArrayList<Data>();
        // where to start
        long actualOffset = elementOffset * gp.elementSize;

        // get the complete buffer
        ByteBuffer dataBuffer = ByteBuffer.allocate(numberToRead * gp.elementSize);
        indexFile.read(actualOffset, dataBuffer);
        dataBuffer.flip();

        byte[] dataArray = new byte[gp.elementSize];
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
    protected IntObjectOpenHashMap<ArrayList<byte[]>> getBucketKeyMapping(byte[]... keys) {
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

        IndexForHeaderIndexFile<Data> index = indexFile.getIndex(); // Pointer to the Index
        int actualChunkIdx = 0, lastChunkIdx = -1;
        long actualChunkOffset = 0, oldChunkOffset = -1;
        int indexInChunk = 0;
        ByteBuffer workingBuffer = ByteBuffer.allocate((int) indexFile.getChunkSize());
        byte[] tmpB = new byte[gp.elementSize]; // stores temporarily the bytestream of an object
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
            result.add((Data) prototype.fromByteBuffer(ByteBuffer.wrap(tmpB)));
            if (indexInChunk == -1) {
                logger.warn("Element with key {} was not found and therefore not updated", key);
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
            HeaderIndexFile<Data> headerIndexFile = new HeaderIndexFile<Data>(gp.databaseDirectory + "/"
                    + hashFunction.getFilename(bucketId), HEADER_FILE_LOCK_RETRY, gp);
            size += headerIndexFile.getFilledUpFromContentStart() / gp.elementSize;
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
        int minElement = indexInChunk / gp.elementSize;
        int numberOfEntries = workingBuffer.limit() / gp.elementSize;

        // binary search
        int maxElement = numberOfEntries - 1;
        int midElement;
        byte comp;
        byte[] tempKey = new byte[gp.keySize];

        while (minElement <= maxElement) {
            midElement = minElement + (maxElement - minElement) / 2;
            indexInChunk = midElement * gp.elementSize;

            workingBuffer.position(indexInChunk);
            workingBuffer.get(tempKey);

            comp = KeyUtils.compareKey(key, tempKey, gp.keySize);
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
     * Instantiates a new {@link DRUMSIterator} and returns it
     * 
     * @return
     */
    public DRUMSIterator<Data> getIterator() {
        return new DRUMSIterator<Data>(hashFunction, gp);
    }

    /**
     * Returns a {@link DRUMS_Reader}. If the {@link DRUMS_Reader} was not instantiated yet, it will be instantiated. If
     * there exists an instance, but the files were already closed, the files will be reopened.<br>
     * <br>
     * Remember, if you have an opened Reader. You can't synchronize buckets.
     * 
     * @return DRUMS_Reader
     * @throws IOException
     * @throws FileLockException
     */
    public DRUMS_Reader<Data> getReader() throws FileLockException, IOException {
        if (reader_instance != null && reader_instance.isClosed) {
            reader_instance.openFiles();
        } else {
            reader_instance = new DRUMS_Reader<Data>(this);
        }
        return reader_instance;
    }

    /** Joins all the DRUMS. */
    public void join() throws InterruptedException {
        syncManager.join();
    }

    /** Closes the DRUMS. */
    public void close() throws InterruptedException {
        if (reader_instance != null) {
            reader_instance.closeFiles();
        }
        reader_instance = null;

        // you can only close a syncmanager, when drums was opened for READ_WRITE
        if (syncManager != null) {
            syncManager.shutdown();
            syncManager.join();
        }
    }

    /**
     * Enables the force mode of DRUMS. All buckets will be synchronized independent from its fill level and last
     * sync-time. <br>
     * Be careful: All buckets will be affected. Even if a bucket contains only one element, the synchronizer is going
     * to synchronize this bucket. <br>
     * This method doesn't block the insert-methods.
     */
    public void enableForceMode() {
        syncManager.startForceMode();
    }

    /**
     * Disable the force mode of DRUMS. Buckets will be synchronized only if constraints like fill-level and last
     * sync-time are fulfilled. <br>
     */
    public void disableForceMode() {
        syncManager.stopForceMode();
    }

    /** Returns the size of one record to store in bytes */
    public int getElementSize() {
        return gp.elementSize;
    }

    /** Returns the size of the key of one record */
    public int getElementKeySize() {
        return gp.keySize;
    }

    /** Returns the underlying hashfunction */
    public AbstractHashFunction getHashFunction() {
        return this.hashFunction;
    }

    /**
     * sets a new HashFunction. Be careful with this method. Overwriting the hashfunction, when elements are already
     * inserted may cause missing elements
     */
    public void setHashFunction(AbstractHashFunction hash) {
        this.hashFunction = hash;
    }

    /** Returns the database-directory */
    public String getDatabaseDirectory() {
        return gp.databaseDirectory;
    }

    /** Returns a pointer to the prototype. This is not a clone. */
    public Data getPrototype() {
        return prototype;
    }

    /** Returns the {@link GlobalParameters} that are used within the {@link DRUMS} */
    public GlobalParameters<Data> getGlobalParameters() {
        return gp;
    }
}
