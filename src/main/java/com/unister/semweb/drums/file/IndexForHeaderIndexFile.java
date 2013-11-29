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
package com.unister.semweb.drums.file;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.Arrays;

import com.unister.semweb.drums.util.ByteArrayComparator;
import com.unister.semweb.drums.util.KeyUtils;

/**
 * This class represents a sparse index for one {@link HeaderIndexFile}. It writes its updates to the given
 * {@link MappedByteBuffer}. A {@link IndexForHeaderIndexFile} knows for each defined chunk the largest key. If the
 * elements are ordered in some manner this allows fast access.<br>
 * <br>
 * <code>
 * Example, 3 Chunks, the third chunk is not fully filled:<br>
 * ------- 10 | ----------- 50 | -- 200 |<br>
 * 1 2 4 5 10 | 11 23 32 49 50 | 51 200 |<br>
 * 
 * </code>
 * 
 * @author Martin Nettling
 */
public class IndexForHeaderIndexFile {

    /** the interal ByteArrayComparator */
    private ByteArrayComparator comparator = new ByteArrayComparator();

    /** chunkId -> largest key in this chunk */
    protected byte maxKeyPerChunk[][];

    /** the size of one chunk */
    protected int chunkSize;

    /** the number of chunks, represented by this index */
    protected int numberOfChunks;

    /** a {@link MappedByteBuffer}, maps to the corresponding byte-region in memory */
    protected ByteBuffer indexBuffer;

    /** reflects the largest initialized chunkIndex */
    private int filledUpTo;

    private int keySize;

    /**
     * This constructor instantiates a new {@link IndexForHeaderIndexFile}. You need to give the number of the indexed
     * chunks, the chunksize and the {@link MappedByteBuffer}, where the index should be written to.
     * 
     * @param numberOfChunks
     *            the expected or maximal number of chunks
     * @param keySize
     * @param chunkSize
     *            the size of one chunk
     * @param indexBuffer
     *            the buffer were to write the indexinformations
     */
    public IndexForHeaderIndexFile(final int numberOfChunks, final int keySize, final int chunkSize,
            final ByteBuffer indexBuffer) {
        this.keySize = keySize;
        this.numberOfChunks = numberOfChunks;
        this.chunkSize = chunkSize;
        this.indexBuffer = indexBuffer;
        this.maxKeyPerChunk = new byte[numberOfChunks][]; // init lookuparray
        this.filledUpTo = 0; // init the actual indexed chunks
        this.fillIndexFromByteBuffer(); // fill eventual in indexBuffer contained informations
    }

    /**
     * loads the index from the {@link MappedByteBuffer}
     */
    protected void fillIndexFromByteBuffer() {
        indexBuffer.rewind();
        for (int i = 0; i < numberOfChunks; i++) {
            maxKeyPerChunk[i] = new byte[keySize];
            indexBuffer.get(maxKeyPerChunk[i]);
            if (KeyUtils.isNull(maxKeyPerChunk[i]) && i > 0) {
                filledUpTo = i - 1;
                break;
            }
        }
    }

    /**
     * returns the position of chunk with the given index in the file.
     * 
     * @param chunkIndex
     * @return long, the byte-offset of the chunk
     */
    public long getStartOffsetOfChunk(int chunkIndex) {
        return chunkIndex * chunkSize;
    }

    /**
     * returns the position in the file, where the chunk, where the key belongs to starts.
     * 
     * @param key
     * @return long, the byte-offset of the chunk
     */
    public long getStartOffsetOfChunkByKey(byte[] key) {
        return getStartOffsetOfChunk(getChunkId(key));
    }

    /**
     * returns the index of the chunk, where the element could be found. If the element can't be in one of the
     * chunks the method will return -1.
     * 
     * @param key
     *            the key to look for
     * @return the id of the chunk, where the key could be found. If there is no putative chunk, then -1 will returned.
     */
    public int getChunkId(byte[] key) {
        int idx = Arrays.binarySearch(maxKeyPerChunk, 0, filledUpTo + 1, key, comparator);
        idx = idx < 0 ? -idx - 1 : idx;
        if (idx > filledUpTo) {
            return -1;
        } else {
            return idx;
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < filledUpTo; i++) {
            sb.append(i + ": " + KeyUtils.toStringUnsignedInt(maxKeyPerChunk[i])).append("\n");
        }
        return sb.toString();
    }

    /**
     * Checks, if this index is consistent. All inserted keys have to be inserted incrementally.
     * 
     * @return boolean
     */
    public boolean isConsistent() {
        for (int i = 1; i < filledUpTo; i++) {
            int comp1 = KeyUtils.compareKey(maxKeyPerChunk[i], maxKeyPerChunk[i - 1]);
            if (comp1 <= 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * Sets a new largest key in the chunk with the given index.
     * 
     * @param chunkIdx
     * @param largestKeyInChunk
     */
    public void setLargestKey(int chunkIdx, byte[] largestKeyInChunk) {
        maxKeyPerChunk[chunkIdx] = largestKeyInChunk;
        filledUpTo = Math.max(filledUpTo, chunkIdx);
        indexBuffer.position(chunkIdx * keySize);
        indexBuffer.put(largestKeyInChunk);
    }
}
