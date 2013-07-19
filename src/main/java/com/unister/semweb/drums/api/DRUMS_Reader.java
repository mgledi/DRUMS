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
import com.unister.semweb.drums.file.FileLockException;
import com.unister.semweb.drums.file.HeaderIndexFile;
import com.unister.semweb.drums.storable.AbstractKVStorable;
import com.unister.semweb.drums.utils.KeyUtils;

/**
 * This class represents an efficient direct access reader. It holds all files opened for reading. Only use this Reader,
 * when there are no write-operations during reading. The files will be locked.
 * 
 * @author m.gleditzsch
 */
public class DRUMS_Reader<Data extends AbstractKVStorable> {
    static Logger logger = LoggerFactory.getLogger(DRUMS_Reader.class);

    /** Marks if files are open. Is set to avoid null-pointer exceptions */
    private boolean filesAreOpend = false;

    /** An array containing all used files. All files are opened when instantiating for performance reasons. */
    private HeaderIndexFile<Data>[] files;

    /** A pointer to the used DRUMS */
    private DRUMS<Data> drums;

    /** An array which knows for each file in <code>files</code> the number of elements. (performance) */
    int[] cumulativeElementsPerFile;

    /** a prototype of Data */
    private Data prototype;

    private int numberOfBuckets;
    private int elementSize;

    /** temporarily used destination buffer */
    private ByteBuffer destBuffer;

    protected boolean isClosed = true;

    protected DRUMS_Reader(DRUMS<Data> drums) throws FileLockException, IOException {
        this.drums = drums;
        this.numberOfBuckets = drums.getHashFunction().getNumberOfBuckets();
        this.elementSize = drums.getElementSize();
        this.prototype = drums.getPrototype();
        openFiles();
    }

    /**
     * Opens all files used by the underlying HashFunction. The pointers are stored in <code>files</code>.
     * 
     * @param path
     * @throws FileLockException
     * @throws IOException
     */
    public void openFiles() throws FileLockException, IOException {
        files = new HeaderIndexFile[numberOfBuckets];
        cumulativeElementsPerFile = new int[numberOfBuckets];
        int lastfile = 0;
        String path = drums.getDatabaseDirectory();
        for (int i = 0; i < numberOfBuckets; i++) {
            String filename = path + "/" + drums.getHashFunction().getFilename(i);
            if (!new File(filename).exists()) {
                cumulativeElementsPerFile[i] = 0;
            } else {
                lastfile = i;
                files[i] = new HeaderIndexFile<Data>(filename, 10, drums.gp);
                cumulativeElementsPerFile[i] = (int) (files[i].getFilledUpFromContentStart() / elementSize);
            }
            if (i > 0) {
                cumulativeElementsPerFile[i] += cumulativeElementsPerFile[i - 1];
            }
        }

        // elementsPerChunk = files[lastfile].getChunkSize() / elementSize;
        destBuffer = ByteBuffer.allocate((int) files[lastfile].getChunkSize());
        filesAreOpend = true;
    }

    /**
     * Returns all elements between lowerKey and upperKey this function is still BUGGY
     */
    public List<Data> getRange(byte[] lowerKey, byte[] upperKey) throws IOException {
        if (!filesAreOpend) {
            throw new IOException("The files are not opened yet. Use openFiles() to open all files.");
        }
        // estimate bounds
        int lowerBucket = drums.getHashFunction().getBucketId(lowerKey);
        int upperBucket = drums.getHashFunction().getBucketId(lowerKey);
        long lowerChunkOffset = files[lowerBucket].getIndex().getStartOffsetOfChunkByKey(lowerKey);
        long upperChunkOffset = files[lowerBucket].getIndex().getStartOffsetOfChunkByKey(upperKey);
        long filesize, startOffset, endOffset;
        byte[] tmpB = new byte[elementSize];

        ArrayList<Data> elements = new ArrayList<Data>();
        // run over all files
        for (int i = lowerBucket; i <= upperBucket; i++) {
            HeaderIndexFile<Data> aktFile = files[i];
            filesize = aktFile.getFilledUpFromContentStart();
            startOffset = 0;
            endOffset = filesize;
            // set the startOffset when we iterate the first file
            if (i == lowerBucket) {
                startOffset = lowerChunkOffset;
            }

            // set the startOffset when we iterate the last file
            if (i == upperBucket) {
                endOffset = Math.max(upperChunkOffset + aktFile.getChunkSize(), filesize);
            }

            while (startOffset < endOffset) {
                destBuffer.clear();
                aktFile.read(startOffset, destBuffer);
                destBuffer.flip();
                while (destBuffer.remaining() > elementSize) {
                    destBuffer.get(tmpB); // get the element
                    Data record = (Data) prototype.fromByteBuffer(ByteBuffer.wrap(tmpB));
                    if (KeyUtils.compareKey(record.key, lowerKey) >= 0 &&
                            KeyUtils.compareKey(record.key, upperKey) <= 0) {
                        elements.add(record);
                    } else if (KeyUtils.compareKey(record.key, upperKey) == 1) {
                        // we have read all relevant elements
                    }
                }
                startOffset += destBuffer.limit();
            }

        }
        return elements;
    }

    /**
     * Returns the element which has exact the key or is the next smallest element after this key
     * 
     * @param key
     * @return
     * @throws IOException
     */
    public Data getPreviousElement(byte[] key) throws IOException {
        if (!filesAreOpend) {
            throw new IOException("The files are not opened yet. Use openFiles() to open all files.");
        }
        return null;
    }

    /**
     * Returns the element which has exact the key or is the next largest element after this key
     * 
     * @param key
     * @return
     * @throws IOException
     */
    public Data getNextElement(byte[] key) throws IOException {
        if (!filesAreOpend) {
            throw new IOException("The files are not opened yet. Use openFiles() to open all files.");
        }
        return null;
    }

    /**
     * Takes a list of keys and searches for that in all buckets.
     * 
     * @param keys
     * @return {@link ArrayList}
     * @throws FileStorageException
     * @throws IOException
     */
    public List<Data> get(long... keys) throws FileStorageException, IOException {
        return this.get(KeyUtils.transformToByteArray(keys));
    }

    /**
     * Takes a list of keys and searches for that in all buckets.
     * 
     * @param keys
     * @return {@link ArrayList}
     * @throws FileStorageException
     * @throws IOException
     */
    public List<Data> get(byte[]... keys) throws FileStorageException, IOException {
        if (!filesAreOpend) {
            throw new IOException("The files are not opened yet. Use openFiles() to open all files.");
        }
        List<Data> result = new ArrayList<Data>();
        IntObjectOpenHashMap<ArrayList<byte[]>> bucketKeyMapping = drums.getBucketKeyMapping(keys);
        for (IntObjectCursor<ArrayList<byte[]>> entry : bucketKeyMapping) {
            ArrayList<byte[]> keyList = entry.value;
            List<Data> readData = drums.searchForData(files[entry.key], keyList.toArray(new byte[keyList.size()][]));
            result.addAll(readData);
        }
        return result;
    }

    /** Closes all files */
    public void closeFiles() {
        filesAreOpend = false;
        for (HeaderIndexFile<Data> file : files) {
            if (file != null) {
                file.close();
            }
        }
    }
}
