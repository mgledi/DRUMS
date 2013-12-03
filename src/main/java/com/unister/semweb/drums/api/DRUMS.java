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
package com.unister.semweb.drums.api;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.unister.semweb.drums.DRUMSParameterSet;
import com.unister.semweb.drums.bucket.Bucket;
import com.unister.semweb.drums.bucket.BucketContainer;
import com.unister.semweb.drums.bucket.BucketContainerException;
import com.unister.semweb.drums.bucket.DynamicMemoryAllocater;
import com.unister.semweb.drums.bucket.hashfunction.AbstractHashFunction;
import com.unister.semweb.drums.file.FileLockException;
import com.unister.semweb.drums.file.HeaderIndexFile;
import com.unister.semweb.drums.file.IndexForHeaderIndexFile;
import com.unister.semweb.drums.storable.AbstractKVStorable;
import com.unister.semweb.drums.storable.GeneralStorable;
import com.unister.semweb.drums.sync.SyncManager;
import com.unister.semweb.drums.sync.synchronizer.ISynchronizerFactory;
import com.unister.semweb.drums.sync.synchronizer.SynchronizerFactory;
import com.unister.semweb.drums.sync.synchronizer.UpdateOnlySynchronizer;
import com.unister.semweb.drums.util.AbstractKVStorableComparator;
import com.unister.semweb.drums.util.ByteArrayComparator;
import com.unister.semweb.drums.util.KeyUtils;

/**
 * An instance of this class provides access to a DRUMS-table. The instance allows managing the storage of
 * {@link AbstractKVStorable}s. Use the static methods in {@link DRUMSInstantiator} to get an instance of {@link DRUMS}.<br>
 * <br>
 * Use the method {@link #insertOrMerge(AbstractKVStorable...)} to insert or merge records.<br>
 * To update records use the method {@link #update(AbstractKVStorable...)}.<br>
 * Single selects can be performed by the method {@link #select(byte[])}.<br>
 * To perform range selects, a {@link DRUMSReader} should be instantiated ({@link #getReader()}.<br>
 * The whole table is scanned best, using an {@link DRUMSIterator} ({@link #getIterator()}.
 * 
 * @author Nils Thieme, Martin Nettling
 * 
 * @param <Data>
 *            an implementation of {@link AbstractKVStorable}, e.g. {@link GeneralStorable}
 */
public class DRUMS<Data extends AbstractKVStorable> {
    private static final Logger logger = LoggerFactory.getLogger(DRUMS.class);

    /** the accessmode for DRUMS */
    public enum AccessMode {
        READ_ONLY, READ_WRITE;
    }

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

    /** A pointer to the {@link DRUMSParameterSet} used by this {@link DRUMS} */
    protected DRUMSParameterSet<Data> gp;

    protected DRUMSReader<Data> reader_instance;

    /**
     * This constructor should only be called by factory methods from this package.
     * 
     * @param hashFunction
     *            a consistent hash-function
     * @param accessMode
     *            can be read or read/write
     * @param gp
     *            contains all needed settings
     * @throws Exception
     */
    protected DRUMS(AbstractHashFunction hashFunction, AccessMode accessMode, DRUMSParameterSet<Data> gp)
            throws IOException {
        this.prototype = gp.getPrototype();
        this.hashFunction = hashFunction;
        this.gp = gp;
        DynamicMemoryAllocater.instantiate(gp);
        gp.MIN_ELEMENT_IN_BUCKET_BEFORE_SYNC = (int) ((gp.BUCKET_MEMORY - gp.BUCKET_MEMORY % gp.MEMORY_CHUNK)
                / hashFunction.getNumberOfBuckets() / prototype.getSize() / 2);
        logger.info("Setted MIN_ELEMENT_IN_BUCKET_BEFORE_SYNC to {}", gp.MIN_ELEMENT_IN_BUCKET_BEFORE_SYNC);
        if (accessMode == AccessMode.READ_WRITE) {
            @SuppressWarnings("unchecked")
            Bucket<Data>[] tmp = new Bucket[hashFunction.getNumberOfBuckets()];
            buckets = tmp;
            for (int i = 0; i < hashFunction.getNumberOfBuckets(); i++) {
                buckets[i] = new Bucket<Data>(i, gp);
                String tmpFileName = gp.DATABASE_DIRECTORY + "/" + hashFunction.getFilename(i);
                if (!new File(tmpFileName).exists()) {
                    HeaderIndexFile<Data> tmpFile;
                    try {
                        tmpFile = new HeaderIndexFile<Data>(tmpFileName, HeaderIndexFile.AccessMode.READ_WRITE, 1, gp);
                        tmpFile.close();
                    } catch (FileLockException e) {
                        logger.error("Can't create file {}, because file is locked by another process.", tmpFileName);
                    } catch (IOException e) {
                        logger.error("Can't create file {}. {}", tmpFileName, e);
                        throw e;
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
     * 
     * @param factory
     */
    public void setSynchronizerFactory(ISynchronizerFactory<Data> factory) {
        this.synchronizerFactory = factory;
        this.syncManager.setSynchronizer(factory);
    }

    /** @return a pointer to the local {@link BucketContainer} */
    public BucketContainer<Data> getBucketContainer() {
        return this.bucketContainer;
    }

    /** @return a pointer to the local {@link SyncManager} */
    public SyncManager<Data> getSyncManager() {
        return this.syncManager;
    }

    /**
     * Adds or merges the given data. If all memory buckets are full, this method is blocking the calling thread. <br>
     * <br>
     * A merge calls the method {@link Data#merge(AbstractKVStorable)}.
     * 
     * @param toPersist
     *            data to insert or update
     * @throws DRUMSException
     *             if an unexpected error occurs
     * @throws InterruptedException
     *             if the call blocks and the current thread is interrupted
     */
    public void insertOrMerge(Data... toPersist) throws DRUMSException, InterruptedException {
        try {
            bucketContainer.addToCache(toPersist);
        } catch (BucketContainerException ex) {
            // This exception should never be thrown because the hash function should map all keys to a bucket.
            throw new DRUMSException(ex);
        }
    }

    /**
     * Updates the given data. Be careful, ONLY updates are provided. If the given array contains elements, which are
     * not already stored in the underlying DRUMS-table, they will be not taken into account during synchronization.<br>
     * <br>
     * This method uses the {@link UpdateOnlySynchronizer}, which by itself uses the Data's implemented update-function
     * ({@link Data#update(AbstractKVStorable)}) to update elements. If you want to merge objects, use
     * {@link #insertOrMerge(Data ...)} instead.
     * 
     * @param records
     *            the data to update
     * 
     * @throws IOException
     */
    public void update(Data... records) throws IOException {
        // ############ reorder data
        IntObjectOpenHashMap<ArrayList<Data>> bucketDataMapping = new IntObjectOpenHashMap<ArrayList<Data>>();
        int bucketId;
        for (Data d : records) {
            bucketId = hashFunction.getBucketId(d.getKey());
            if (!bucketDataMapping.containsKey(bucketId)) {
                bucketDataMapping.put(bucketId, new ArrayList<Data>());
            }
            bucketDataMapping.get(bucketId).add(d);
        }

        for (IntObjectCursor<ArrayList<Data>> entry : bucketDataMapping) {
            UpdateOnlySynchronizer<Data> synchronizer = new UpdateOnlySynchronizer<Data>(gp.DATABASE_DIRECTORY + "/"
                    + hashFunction.getFilename(entry.key), gp);
            @SuppressWarnings("unchecked")
            Data[] toUpdate = (Data[]) entry.value.toArray(new AbstractKVStorable[entry.value.size()]);
            Arrays.sort(toUpdate, new AbstractKVStorableComparator());
            synchronizer.upsert(toUpdate);
        }
    }

    /**
     * Selects all existing records to the keys in the given array.
     * 
     * @param keys
     *            the keys to look for
     * @return a list of all found elements
     * @throws DRUMSException
     */
    public List<Data> select(byte[]... keys) throws DRUMSException {
        List<Data> result = new ArrayList<Data>();
        IntObjectOpenHashMap<ArrayList<byte[]>> bucketKeyMapping = getBucketKeyMapping(keys);
        String filename;
        for (IntObjectCursor<ArrayList<byte[]>> entry : bucketKeyMapping) {
            filename = gp.DATABASE_DIRECTORY + "/" + hashFunction.getFilename(entry.key);
            HeaderIndexFile<Data> indexFile = null;
            try {
                indexFile = new HeaderIndexFile<Data>(filename, HeaderIndexFile.AccessMode.READ_ONLY,
                        gp.HEADER_FILE_LOCK_RETRY, gp);

                ArrayList<byte[]> keyList = entry.value;
                result.addAll(searchForData(indexFile, keyList.toArray(new byte[keyList.size()][])));
            } catch (FileLockException ex) {
                logger.error("Could not access the file {} within {} retries. The file seems to be locked.", filename,
                        gp.HEADER_FILE_LOCK_RETRY);
                throw new DRUMSException(ex);
            } catch (IOException ex) {
                logger.error("An exception occurred while trying to get objects from the file {}.", filename, ex);
                throw new DRUMSException(ex);
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
        String filename = gp.DATABASE_DIRECTORY + "/" + hashFunction.getFilename(bucketId);
        HeaderIndexFile<Data> indexFile = new HeaderIndexFile<Data>(filename, HeaderIndexFile.AccessMode.READ_ONLY,
                gp.HEADER_FILE_LOCK_RETRY, gp);

        List<Data> result = new ArrayList<Data>();
        // where to start
        long actualOffset = elementOffset * gp.getElementSize();

        // get the complete buffer
        ByteBuffer dataBuffer = ByteBuffer.allocate(numberToRead * gp.getElementSize());
        indexFile.read(actualOffset, dataBuffer);
        dataBuffer.flip();

        byte[] dataArray = new byte[gp.getElementSize()];
        while (dataBuffer.position() < dataBuffer.limit()) {
            dataBuffer.get(dataArray);
            Data copy = prototype.fromByteBuffer(ByteBuffer.wrap(dataArray));
            result.add(copy);
        }
        indexFile.close();
        return result;
    }

    /**
     * This method maps all keys in the given array to their corresponding buckets and returns the determined mapping.
     * 
     * @param keys
     *            the keys to search for
     * @return a mapping from bucket-id to a list of keys.
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
     * Searches for the {@link AbstractKVStorable}-records corresponding the given keys within the given indexFile. This
     * is done by using the {@link IndexForHeaderIndexFile} from the given {@link HeaderIndexFile}. If you want to do
     * this in a more sequential way, try to use the method {@link #read(int, int, int)} or use an {@link DRUMSIterator}
     * . ({@link #getIterator()})
     * 
     * @param indexFile
     *            {@link HeaderIndexFile}, where to search for the keys
     * @param keys
     *            the keys to search for
     * @return an {@link ArrayList} which contains the found records. Can be less than the number of requested keys.
     * @throws IOException
     */
    public List<Data> searchForData(HeaderIndexFile<Data> indexFile, byte[]... keys) throws IOException {
        Arrays.sort(keys, new ByteArrayComparator());
        List<Data> result = new ArrayList<Data>();

        IndexForHeaderIndexFile index = indexFile.getIndex(); // Pointer to the Index
        int actualChunkIdx = 0, lastChunkIdx = -1;
        long actualChunkOffset = 0, oldChunkOffset = -1;
        int indexInChunk = 0;
        ByteBuffer workingBuffer = ByteBuffer.allocate((int) indexFile.getChunkSize());
        byte[] tmpB = new byte[gp.getElementSize()]; // stores temporarily the bytestream of an object
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
            Data copy = prototype.fromByteBuffer(ByteBuffer.wrap(tmpB));
            result.add(copy);
            if (indexInChunk == -1) {
                logger.warn("Element with key {} was not found.", key);
                indexInChunk = 0;
            }
            lastChunkIdx = actualChunkIdx; // remember last chunk
            oldChunkOffset = actualChunkOffset; // remember last offset of the last chunk
        }
        return result;
    }

    /**
     * @return the number of elements in the database.
     * @throws IOException
     * @throws FileLockException
     */
    public long size() throws FileLockException, IOException {
        long size = 0L;
        for (int bucketId = 0; bucketId < hashFunction.getNumberOfBuckets(); bucketId++) {
            HeaderIndexFile<Data> headerIndexFile = new HeaderIndexFile<Data>(gp.DATABASE_DIRECTORY + "/"
                    + hashFunction.getFilename(bucketId), gp.HEADER_FILE_LOCK_RETRY, gp);
            size += headerIndexFile.getFilledUpFromContentStart() / gp.getElementSize();
            headerIndexFile.close();
        }
        return size;
    }

    /**
     * Searches for the given key in workingBuffer, beginning at the given index. Remember: The records in the
     * given workingBuffer have to be ordered ascending.
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
        int minElement = indexInChunk / gp.getElementSize();
        int numberOfEntries = workingBuffer.limit() / gp.getElementSize();

        // binary search
        int maxElement = numberOfEntries - 1;
        int midElement;
        int comp;
        byte[] tempKey = new byte[gp.getKeySize()];

        while (minElement <= maxElement) {
            midElement = minElement + (maxElement - minElement) / 2;
            indexInChunk = midElement * gp.getElementSize();

            workingBuffer.position(indexInChunk);
            workingBuffer.get(tempKey);

            comp = KeyUtils.compareKey(key, tempKey, gp.getKeySize());
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
     * Instantiates a new {@link DRUMSIterator} and returns it.
     * 
     * @return a new {@link DRUMSIterator}
     */
    public DRUMSIterator<Data> getIterator() {
        return new DRUMSIterator<Data>(hashFunction, gp);
    }

    /**
     * Returns a {@link DRUMSReader}. If the {@link DRUMSReader} was not instantiated yet, it will be instantiated. If
     * there exists an instance, but the files were already closed, the files will be reopened.<br>
     * <br>
     * Remember, if you have an opened Reader. You can't synchronize buckets.
     * 
     * @return a pointer to the internal {@link DRUMSReader}-instance
     * @throws IOException
     * @throws FileLockException
     */
    public DRUMSReader<Data> getReader() throws FileLockException, IOException {
        if (reader_instance != null && !reader_instance.filesAreOpened) {
            reader_instance.openFiles();
        } else {
            reader_instance = new DRUMSReader<Data>(this);
        }
        return reader_instance;
    }

    /**
     * Joins all the DRUMS-table.
     * 
     * @throws InterruptedException
     */
    public void join() throws InterruptedException {
        syncManager.join();
    }

    /**
     * Closes the DRUMS.
     * 
     * @throws InterruptedException
     */
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

    /** @return the size of one record to store in bytes */
    public int getElementSize() {
        return gp.getElementSize();
    }

    /** @return the size of the key of one record */
    public int getElementKeySize() {
        return gp.getKeySize();
    }

    /** @return the underlying hash-function */
    public AbstractHashFunction getHashFunction() {
        return this.hashFunction;
    }

    /**
     * sets a new HashFunction. Be careful with this method. Overwriting the hashfunction, when elements are already
     * inserted may cause, that you won't find those elements by select.
     * 
     * @param hashfunction
     *            the hash-function to set
     */
    public void setHashFunction(AbstractHashFunction hashfunction) {
        this.hashFunction = hashfunction;
    }

    /** @return the database-directory */
    public String getDatabaseDirectory() {
        return gp.DATABASE_DIRECTORY;
    }

    /** @return a pointer to the prototype. This is not a clone. */
    public Data getPrototype() {
        return prototype;
    }

    /** @return the {@link DRUMSParameterSet} that are used within the {@link DRUMS} */
    public DRUMSParameterSet<Data> getGlobalParameters() {
        return gp;
    }
}
