package com.unister.semweb.sdrum.synchronizer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.unister.semweb.sdrum.bucket.Bucket;
import com.unister.semweb.sdrum.file.AbstractHeaderFile.AccessMode;
import com.unister.semweb.sdrum.file.FileLockException;
import com.unister.semweb.sdrum.file.HeaderIndexFile;
import com.unister.semweb.sdrum.file.IndexForHeaderIndexFile;
import com.unister.semweb.sdrum.storable.AbstractKVStorable;
import com.unister.semweb.sdrum.utils.KeyUtils;

/**
 * Takes a list of {@link AbstractKVStorable} and synchronizes them with a file. ONLY update are supported.
 * The core assumption is that the list of {@link AbstractKVStorable} and the entries in the file are sorted ascended.
 * 
 * @author n.thieme, m.gleditzsch
 */
public class UpdateOnlySynchronizer<Data extends AbstractKVStorable<Data>> {
    /** */
    private static Logger log = LoggerFactory.getLogger(UpdateOnlySynchronizer.class);

    /** The file instance from which we receive a {@link FileChannel}. */
    protected String dataFilename;

    /** a pointer to the file, where data-objects are stored */
    protected HeaderIndexFile<Data> dataFile;

    /** the header of the bucket, something like an index */
    private IndexForHeaderIndexFile header;

    /** a prototype element of type Data */
    Data prototype;

    /** the ByteBuffer to work on. Is used for reading and writing, could be replaced by a MappedByteBuffer */
    ByteBuffer workingBuffer;

    /** the size of one Data-object. For fast access only */
    private int elementSize;

    /**
     * This method constructs a {@link Synchronizer}. The name of the file were to write the elements to have to be
     * given.
     * 
     * @param dataFilename
     * @param header
     */
    public UpdateOnlySynchronizer(final String dataFilename, final Data prototype) {
        this.dataFilename = dataFilename;
        this.prototype = prototype;
        this.elementSize = prototype.getByteBufferSize();
        try {
            /* Another thread can have access to this file in parallel. So we must wait to get exclusive access. */
            dataFile = new HeaderIndexFile<Data>(dataFilename, AccessMode.READ_WRITE, Integer.MAX_VALUE,
                    prototype.key.length, elementSize);
            header = dataFile.getIndex(); // Pointer to the Index
        } catch (FileLockException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        this.workingBuffer = ByteBuffer.allocate(dataFile.getChunkSize());
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
                actualChunkIdx = header.getChunkId(toUpdate[i].key);
                actualChunkOffset = header.getStartOffsetOfChunk(actualChunkIdx);

                if (actualChunkOffset > dataFile.getFilledUpFromContentStart()) {
                    log.warn("Element with key {} was not found. Chunk {} does not exist.", actualChunkIdx, toUpdate[i].key);
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

                indexInChunk = updateElementInReadBuffer(toUpdate[i], indexInChunk);
                if (indexInChunk == -1) {
                    log.warn("Element with key {} was not found and therefore not updated.", toUpdate[i].key);
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
        int minElement = indexInChunk / elementSize;
        int numberOfEntries = workingBuffer.limit() / elementSize;
        byte[] actualKey = data.key;
        // binary search
        int maxElement = numberOfEntries - 1;
        int midElement;
        byte compare;
        byte[] tmpKey = new byte[actualKey.length];
        while (minElement <= maxElement) {
            midElement = minElement + (maxElement - minElement) / 2;
            indexInChunk = midElement * elementSize;
            workingBuffer.position(indexInChunk);
            workingBuffer.get(tmpKey);
            compare = KeyUtils.compareKey(actualKey, tmpKey);
            if (compare == 0) {
                // first read the old element
                workingBuffer.position(indexInChunk);
                byte[] b = new byte[elementSize];
                workingBuffer.get(b);
                Data toUpdate = prototype.fromByteBuffer(ByteBuffer.wrap(b));
                // update the old element and writ it
                toUpdate.update(data);
                workingBuffer.position(indexInChunk);
                workingBuffer.put(data.toByteBuffer());
                return indexInChunk;
            } else if (compare == -1) {
                maxElement = midElement - 1;
            } else {
                minElement = midElement + 1;
            }
        }
        return -1;
    }
}
