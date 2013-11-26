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
package com.unister.semweb.drums.synchronizer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.unister.semweb.drums.GlobalParameters;
import com.unister.semweb.drums.bucket.Bucket;
import com.unister.semweb.drums.file.FileLockException;
import com.unister.semweb.drums.file.HeaderIndexFile;
import com.unister.semweb.drums.file.IndexForHeaderIndexFile;
import com.unister.semweb.drums.file.AbstractHeaderFile.AccessMode;
import com.unister.semweb.drums.storable.AbstractKVStorable;
import com.unister.semweb.drums.utils.KeyUtils;

/**
 * Takes a list of {@link AbstractKVStorable} and synchronizes them with a file. ONLY update are supported. The core
 * assumption is that the list of {@link AbstractKVStorable} and the entries in the file are sorted ascended.
 * 
 * @author Nils Thieme, Martin Nettling
 * @param <Data>
 *            an implementation of AbstarctKVStorable
 */
public class UpdateOnlySynchronizer<Data extends AbstractKVStorable> {
    /** */
    private static Logger log = LoggerFactory.getLogger(UpdateOnlySynchronizer.class);

    /** The file instance from which we receive a {@link FileChannel}. */
    protected String dataFilename;

    /** a pointer to the file, where data-objects are stored */
    protected HeaderIndexFile<Data> dataFile;

    /** the header of the bucket, something like an index */
    private IndexForHeaderIndexFile header;

    /** a prototype element of type Data */
    private Data prototype;

    /** the ByteBuffer to work on. Is used for reading and writing, could be replaced by a MappedByteBuffer */
    private ByteBuffer workingBuffer;

    /** A pointer to the global Parameters */
    private GlobalParameters<Data> gp;

    /**
     * This method constructs a {@link Synchronizer}. The name of the file were to write the elements to have to be
     * given.
     * 
     * @param dataFilename
     * @param gp
     * 
     */
    public UpdateOnlySynchronizer(final String dataFilename, final GlobalParameters<Data> gp) {
        this.gp = gp;
        this.dataFilename = dataFilename;
        this.prototype = gp.getPrototype();
        try {
            /* Another thread can have access to this file in parallel. So we must wait to get exclusive access. */
            dataFile = new HeaderIndexFile<Data>(dataFilename, AccessMode.READ_WRITE, Integer.MAX_VALUE, gp);
            header = dataFile.getIndex(); // Pointer to the Index
        } catch (FileLockException ex) {
            log.error("Could not aquire the log for the file to update: {}", dataFilename, ex);
            throw new IllegalStateException(ex);
        } catch (IOException ex) {
            log.error("Error occurred while initialising UpdateOnlySynchronizer: {}", dataFilename, ex);
            throw new IllegalStateException(ex);
        }

        this.workingBuffer = ByteBuffer.allocate((int) dataFile.getChunkSize());
    }

    /**
     * This method handles the given {@link AbstractKVStorable}s from a {@link Bucket}. It provides updates and inserts
     * of those objects knowing all already stored {@link AbstractKVStorable}s in the <code>dataFile</code>.
     * 
     * @param toUpdate
     * @throws IOException
     */
    public void upsert(Data[] toUpdate) throws IOException {
        try {
            int actualChunkIdx = 0, lastChunkIdx = -1;
            long actualChunkOffset = 0, oldChunkOffset = -1;
            int indexInChunk = 0;
            // special case
            for (int i = 0; i < toUpdate.length; i++) {
                // get actual chunkIndex
                actualChunkIdx = header.getChunkId(toUpdate[i].getKey());
                actualChunkOffset = header.getStartOffsetOfChunk(actualChunkIdx);

                if (actualChunkOffset > dataFile.getFilledUpFromContentStart()) {
                    log.warn("Element with key {} was not found. Chunk {} does not exist.", actualChunkIdx,
                            toUpdate[i].getKey());
                    continue;
                }

                // if it is the same chunk as in the last step, use the old readbuffer
                if (actualChunkIdx != lastChunkIdx) {
                    // if we have read a chunk
                    if (oldChunkOffset > -1) {
                        dataFile.write(oldChunkOffset, workingBuffer);
                        indexInChunk = 0;
                    }
                    // read a new part to the readBuffer
                    dataFile.read(actualChunkOffset, workingBuffer);
                }

                int oldIndexInChunk = indexInChunk;
                indexInChunk = updateElementInReadBuffer(toUpdate[i], indexInChunk);
                if (indexInChunk == -1) {
                    log.warn(
                            "Element with key {} was not found and therefore not updated. File: {}, Chunk: {}, Index for searching in buffer: {}, Index after update: {}",
                            new Object[] { toUpdate[i].getKey(), this.dataFilename, actualChunkIdx, oldIndexInChunk,
                                    indexInChunk });
                    indexInChunk = 0;
                }

                lastChunkIdx = actualChunkIdx; // remember last chunk
                oldChunkOffset = actualChunkOffset; // remember last offset of the last chunk
            }
            // if we have read a chunk
            if (oldChunkOffset > -1) {
                dataFile.write(oldChunkOffset, workingBuffer);
            }

        } finally {
            // close the file
            dataFile.close();
        }
    }

    /** traverses the readBuffer */
    private int updateElementInReadBuffer(Data data, int indexInChunk) {
        workingBuffer.position(indexInChunk);
        int minElement = indexInChunk / gp.elementSize;
        int numberOfEntries = workingBuffer.limit() / gp.elementSize;
        byte[] actualKey = data.getKey();
        // binary search
        int maxElement = numberOfEntries - 1;
        int midElement;
        int compare;
        byte[] tmpKey = new byte[actualKey.length];
        while (minElement <= maxElement) {
            midElement = minElement + (maxElement - minElement) / 2;
            indexInChunk = midElement * gp.elementSize;
            workingBuffer.position(indexInChunk);
            workingBuffer.get(tmpKey);
            compare = KeyUtils.compareKey(actualKey, tmpKey);
            if (compare == 0) {
                // first read the old element
                workingBuffer.position(indexInChunk);
                byte[] b = new byte[gp.elementSize];
                workingBuffer.get(b);
                Data toUpdate = prototype.fromByteBuffer(ByteBuffer.wrap(b));
                // update the old element and write it
                toUpdate.update(data);
                workingBuffer.position(indexInChunk);
                workingBuffer.put(data.toByteBuffer());
                return indexInChunk;
            } else if (compare < 0) {
                maxElement = midElement - 1;
            } else {
                minElement = midElement + 1;
            }
        }
        return -1;
    }
}
