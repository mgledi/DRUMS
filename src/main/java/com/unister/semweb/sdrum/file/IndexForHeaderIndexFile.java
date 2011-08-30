package com.unister.semweb.sdrum.file;

import java.nio.MappedByteBuffer;

import com.unister.semweb.sdrum.utils.KeyUtils;

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
 * 
 * @author m.gleditzsch
 * 
 */
public class IndexForHeaderIndexFile {

    /** chunkId -> largest key in this chunk */
    protected byte maxKeyPerChunk[][];

    /** the size of one chunk */
    protected int chunkSize;

    /** the number of chunks, represented by this index */
    protected int numberOfChunks;

    /** a {@link MappedByteBuffer}, maps to the corresponding byte-region in memory */
    protected MappedByteBuffer indexBuffer;

    /** reflects the largest initialized chunkIndex */
    private int filledUpTo;

    private int keySize;
    /**
     * This constructor instantiates a new {@link IndexForHeaderIndexFile}. You need to give the number of the indexed
     * chunks, the chunksize and the {@link MappedByteBuffer}, where the index should be written to.
     * 
     * @param <b>int numberOfChunks</b> the expected or maximal number of chunks
     * @param <b>int chunkSize</b>, the size of one chunk
     * @param <b>MappedByteBuffer indexBuffer</b>, the buffer were to write the indexinformations
     */
    public IndexForHeaderIndexFile(final int numberOfChunks, final int chunkSize, final MappedByteBuffer indexBuffer) {
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
        keySize = indexBuffer.getInt();
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
     * 
     * @return long, the byte-offset of the chunk
     */
    public long getStartOffsetOfChunk(int chunkIndex) {
        return chunkIndex * chunkSize;
    }

    /**
     * returns the position in the file, where the chunk, where the key belongs to starts.
     * 
     * @param key
     * 
     * @return long, the byte-offset of the chunk
     */
    public long getStartOffsetOfChunkByKey(byte[] key) {
        return getStartOffsetOfChunk(getChunkId(key));
    }

    /**
     * returns the index of the chunk, where the element could be find. If the element cant be in one chunk the function
     * returns -1
     * 
     * @param long key, the key of the element to find
     * @return
     */
    public int getChunkId(byte[] key) {
        // adapted binary search
        int minElement = 0, maxElement = filledUpTo, midElement;
        byte comp1, comp2;
        while (minElement <= maxElement) {
            midElement = minElement + (maxElement - minElement) / 2;
            comp2 = KeyUtils.compareKey(key, maxKeyPerChunk[midElement]);
            if(midElement == 0) {
                if(comp2 > 0 ) {
                    return 1;
                }
                return 0;
            }
            comp1 = KeyUtils.compareKey(maxKeyPerChunk[midElement - 1], key);
            if (comp1 < 0 && comp2 <= 0) {
                return midElement;
            } else if (comp1 > 0) {
                minElement = midElement + 1;
            } else {
                maxElement = midElement - 1;
            }
        }
        return -1;
    }

    /**
     * Sets a new largest key in the chunk with the given index.
     * 
     * @param chunkIdx
     * @param largestKeyInChunk
     */
    public void setLargestKey(int chunkIdx, byte[] largestKeyInChunk) {
        // System.out.println(maxKeyPerChunk.length + " <? " + chunkIdx);
        maxKeyPerChunk[chunkIdx] = largestKeyInChunk;
        filledUpTo = Math.max(filledUpTo, chunkIdx);
        indexBuffer.position(chunkIdx * largestKeyInChunk.length + 4); // +4 cause of keySize stored in this buffer
        indexBuffer.put(largestKeyInChunk);
    }
}
