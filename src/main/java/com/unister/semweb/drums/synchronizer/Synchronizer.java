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
package com.unister.semweb.drums.synchronizer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.unister.semweb.drums.GlobalParameters;
import com.unister.semweb.drums.bucket.Bucket;
import com.unister.semweb.drums.file.FileLockException;
import com.unister.semweb.drums.file.HeaderIndexFile;
import com.unister.semweb.drums.file.IndexForHeaderIndexFile;
import com.unister.semweb.drums.file.AbstractHeaderFile.AccessMode;
import com.unister.semweb.drums.storable.AbstractKVStorable;
import com.unister.semweb.drums.sync.SyncThread;
import com.unister.semweb.drums.utils.KeyUtils;

/**
 * Takes a list of {@link AbstractKVStorable} and synchronizes them with a file. A {@link Synchronizer} is instantiated
 * by a {@link SyncThread}. The core assumption is that the list of {@link AbstractKVStorable} and the entries in the
 * file are sorted ascended.
 * 
 * @author Nils Thieme, Martin Gleditzsch
 */
public class Synchronizer<Data extends AbstractKVStorable> {
    private static final Logger log = LoggerFactory.getLogger(Synchronizer.class);

    /** The file instance from which we receive a {@link FileChannel}. */
    protected String dataFilename;

    protected HeaderIndexFile<Data> dataFile;

    /** Number of entries that are read into memory. */
    protected int numberOfEntriesInOneChunk;

    /** Actual position in the file from which to read data. */
    protected long readOffset;

    /** Actual position in the file where to write the data. */
    protected long writeOffset;

    /** a buffer for writing always a bunch of elements */
    private ByteBuffer bufferedWriter;

    /** a buffer for reading a chunk (with elements) */
    private ByteBuffer bufferedReader;

    /** the header of the bucket, something like an index */
    private IndexForHeaderIndexFile<Data> header;

    /** the largest key in the actual chunk for writing */
    private byte[] largestKeyInChunk;

    /** The number of entries that were added to the file. */
    private long numberOfInsertedEntries;

    /** The number of entries that were updated within the file. */
    private long numberOfUpdateEntries;
    private long numberOfOldEntries = 0;

    LinkedList<byte[]> pendingElements = new LinkedList<byte[]>();
    private long filledUpToWhenStarted;

    // TODO
    AbstractKVStorable prototype;

    // TODO: comment
    private int elementSize;

    /** A Pointer to the GlobalParameters used by the DRUMS containing this Synchronizer */
    GlobalParameters<Data> gp;

    /**
     * This method constructs a {@link Synchronizer}. The name of the file were to write the elements to have to be
     * given.
     * 
     * @param dataFilename
     *            the file, where to store the {@link AbstractKVStorable}
     * @param gp
     *            a pointer to the {@link GlobalParameters}
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public Synchronizer(String dataFilename, GlobalParameters<Data> gp) {
        this.gp = gp;
        this.prototype = gp.getPrototype();
        this.dataFilename = dataFilename;
        this.elementSize = prototype.getByteBufferSize();
        this.numberOfEntriesInOneChunk = (int) Math.floor(gp.SYNC_CHUNK_SIZE / elementSize);
        this.bufferedWriter = ByteBuffer.allocate(numberOfEntriesInOneChunk * elementSize);
        this.bufferedReader = ByteBuffer.allocate(numberOfEntriesInOneChunk * elementSize);
    }

    /**
     * This method handles the given {@link AbstractKVStorable}s from a {@link Bucket}. It provides updates and inserts
     * of those objects knowing all already stored {@link AbstractKVStorable}s in the <code>dataFile</code>.
     * 
     * @param toAdd
     * @throws IOException
     */
    public void upsert(AbstractKVStorable[] toAdd) throws IOException {
        try {
            /* Another thread can have access to this file in parallel. So we must wait to get exclusive access. */
            dataFile = new HeaderIndexFile<Data>(
                    dataFilename,
                    AccessMode.READ_WRITE,
                    Integer.MAX_VALUE,
                    gp
                    );
            header = dataFile.getIndex(); // Pointer to the Index
        } catch (FileLockException e) {
            log.error("Errror occurred while opening database file.", e);
        }
        try {
            toAdd = AbstractKVStorable.merge(toAdd);

            readOffset = 0;
            filledUpToWhenStarted = dataFile.getFilledUpFromContentStart(); // need to remember how a many "old" bytes
                                                                            // were written in the file (will be
                                                                            // overwritten)
            writeOffset = 0; // at this position we want to start writing

            // readFirstChunkFromFile(toAdd[0]); // We read the chunk, where the first element has to be inserted or
            // updated from the disk
            readNextChunkFromFile();

            // We take one AbstractKVStorable from the chunk. The chunk will be automatically incremented
            byte[] dateFromDisk = getFromDisk();

            int indexOfToAdd = 0;
            int keyLength = prototype.getKey().length;

            // handle all AbstractKVStorable (update or insert)
            byte compare;
            for (indexOfToAdd = 0; indexOfToAdd < toAdd.length && dateFromDisk != null;) {
                AbstractKVStorable dateFromBucket = toAdd[indexOfToAdd];
                compare = KeyUtils.compareKey(dateFromBucket.key, dateFromDisk, keyLength);

                /* insert element from bucket */
                if (compare == -1) {
                    write(dateFromBucket.toByteBuffer().array(), false); // write date
                    indexOfToAdd++;// next dateFromBucket

                    // Incrementing the number of inserted entries.
                    numberOfInsertedEntries++;
                    continue;
                }

                /* merges element from bucket and element from disk */
                if (compare == 0) {
                    prototype.initFromByteBuffer(ByteBuffer.wrap(dateFromDisk));
                    AbstractKVStorable newDate = prototype.merge(dateFromBucket);

                    // TODO: think about update
                    write(newDate.toByteBuffer().array(), true); // write date
                    indexOfToAdd++; // next dateFromBucket
                    dateFromDisk = getFromDisk(); // get next date from disk

                    // Incrementing the number of updated entries.
                    numberOfUpdateEntries++;
                    continue;
                }

                /* insert element from disk (pendingElements) */
                if (compare == 1) {
                    prototype.initFromByteBuffer(ByteBuffer.wrap(dateFromDisk));
                    // if the dateFromDisk was marked as deleted
                    if (!prototype.isMarkedAsDeleted()) {
                        write(dateFromDisk, true); // write date
                        dateFromDisk = getFromDisk(); // get next date from disk
                        numberOfOldEntries++;
                    }
                    continue;
                }
            }

            // end of the already stored elements, but there are still elements from bucket to insert
            if (dateFromDisk == null) {
                for (; indexOfToAdd < toAdd.length; indexOfToAdd++) {
                    if (write(toAdd[indexOfToAdd].toByteBuffer().array(), false)) {
                        numberOfInsertedEntries++; // Incrementing the number of inserted entries.
                    }
                }
            }

            // all entries from bucket were added, but there are still pending elements .
            while (dateFromDisk != null) {
                write(dateFromDisk, true);
                dateFromDisk = getFromDisk();
            }

            // write the remaining elements from the bufferedWriter to the disk
            this.writeBuffer(); // TODO: check if the data fits into one chunk

            int lastChunkId = dataFile.getChunkIndex(writeOffset + bufferedWriter.position());
            this.header.setLargestKey(lastChunkId, largestKeyInChunk);
        } finally {
            // close the file
            dataFile.close();
        }
    }

    /**
     * writes the given {@link byte[]} to the given {@link FileChannel}. This method improves performance by 30%,
     * because it is not needed anymore to create all AbstractKVStorable from byte-arrays. The given byte-array contains
     * only one {@link AbstractKVStorable}.
     * 
     * @param newData
     * @param key
     * @param alreadyExist
     * @throws IOException
     */
    protected boolean write(byte[] newData, boolean alreadyExist) throws IOException {
        // If the data is invalid.
        if (KeyUtils.isNull(newData, prototype.key.length)) {
            // if(alreadyExist) System.err.println("invalid from disk");
            return false;
        }
        // if the last readChunk was full
        ByteBuffer toAdd = ByteBuffer.wrap(newData);
        long positionOfToAddInFile = writeOffset + bufferedWriter.position();
        bufferedWriter.put(toAdd);

        largestKeyInChunk = Arrays.copyOfRange(newData, 0, prototype.key.length); // elements are stored ordered so we
                                                                                  // can easily remember the largest key
        int chunkId = dataFile.getChunkIndex(positionOfToAddInFile);
        header.setLargestKey(chunkId, largestKeyInChunk);
        if (bufferedWriter.remaining() == 0) {
            writeBuffer();
        }
        return true;
    }

    /**
     * writes the remaining bytes in bufferedWriter to the given FileChannel
     * 
     * @param FileChannel
     * @throws IOException
     */
    private void writeBuffer() throws IOException {
        bufferedWriter.flip(); // position is set to zero in dataFile.write
        dataFile.write(writeOffset, bufferedWriter);
        writeOffset += bufferedWriter.limit();
        bufferedWriter.clear();
        readNextChunkFromFile(); // before overwriting the next bucket. Read it.
    }

    /**
     * Gets the next {@link AbstractKVStorable} from disk or from the cache. If the end of the cache is reached a new
     * read will be made from the file.
     * 
     * @return
     * @throws IOException
     */
    private byte[] getFromDisk() throws IOException {
        if (!pendingElements.isEmpty()) {
            return pendingElements.pop();
        }
        return null;
    }

    /**
     * Reads one chunk from the file. It is possible that the chunk is not complete because the end of the file is
     * reached. In this case the maximum number of {@link AbstractKVStorable} will be retrieved.
     * 
     * @return a chunk from the file
     * @throws IOException
     */
    private boolean readNextChunkFromFile() throws IOException {
        if (readOffset >= filledUpToWhenStarted || readOffset < 0) {
            return false;
        }

        bufferedReader.clear();
        dataFile.read(readOffset, bufferedReader);
        bufferedReader.position(0);
        if (bufferedReader.limit() > 0) {
            readOffset += bufferedReader.limit();
            while (bufferedReader.remaining() > 0) {
                byte[] dst = new byte[elementSize];
                bufferedReader.get(dst);
                pendingElements.add(dst);
            }
        }
        return true;
    }

    @SuppressWarnings("unused")
    private boolean readFirstChunkFromFile(AbstractKVStorable toAdd) throws IOException {
        readOffset = header.getStartOffsetOfChunkByKey(toAdd.getKey());
        if (readOffset < 0) {
            readOffset = 0;
        }
        return readNextChunkFromFile();
    }

    /** Closes, if not yet closed, the dataFile */
    public void close() {
        if (dataFile != null && dataFile.isOpen()) {
            dataFile.close();
        }
    }

    /* Measurement methods */
    /** Returns the number of entries that were inserted. */
    public long getNumberOfInsertedEntries() {
        return numberOfInsertedEntries;
    }

    /** Returns the number of entries that were updated in the file. */
    public long getNumberOfUpdatedEntries() {
        return numberOfUpdateEntries;
    }
}