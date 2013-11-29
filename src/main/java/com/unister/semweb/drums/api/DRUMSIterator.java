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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.unister.semweb.drums.GlobalParameters;
import com.unister.semweb.drums.bucket.hashfunction.AbstractHashFunction;
import com.unister.semweb.drums.file.FileLockException;
import com.unister.semweb.drums.file.HeaderIndexFile;
import com.unister.semweb.drums.storable.AbstractKVStorable;
import com.unister.semweb.drums.storable.GeneralStorable;

/**
 * An instance of this class provides a Read-Only-Iterator for a given DRUMS-table. During iteration, no elements should
 * be inserted by another process.
 * 
 * @author Martin Nettling
 * @param <Data>
 *            an implementation of {@link AbstractKVStorable}, e.g. {@link GeneralStorable}
 */
public class DRUMSIterator<Data extends AbstractKVStorable> implements Iterator<Data>, Closeable {
    private static Logger logger = LoggerFactory.getLogger(DRUMSIterator.class);
    /** The hash function. Maps an element to a bucket. */
    private AbstractHashFunction hashFunction;

    /** a prototype of the elements to handle */
    private Data prototype;

    /** a pointer to the actual file */
    private HeaderIndexFile<Data> actualFile;

    /** the temporary readBuffer. The size of a read-Chunk is coded in {@link HeaderIndexFile} */
    private ByteBuffer readBuffer;

    /** the actual bucket we handle */
    private int actualBucketId = 0;

    /** the actual file offset of the actual bucket */
    private long actualFileOffset = 0;

    /** for fast access a destination buffer. */
    byte[] curDestBuffer;

    /** for fast access, the number of buckets */
    private int numberOfBuckets = 0;

    /** counts all elements read by this iterator */
    private long countElementsRead = 0;

    /** A pointer to the GlobalParameters used by this DRUMS */
    final protected GlobalParameters<Data> gp;

    /**
     * Initializes the iterator with the hash function and the global parameters.
     * 
     * @param hashFunction
     * @param globalparameters
     */
    public DRUMSIterator(AbstractHashFunction hashFunction, GlobalParameters<Data> globalparameters) {
        this.gp = globalparameters;
        this.prototype = globalparameters.getPrototype();
        this.hashFunction = hashFunction;
        this.curDestBuffer = new byte[globalparameters.elementSize];
        this.numberOfBuckets = hashFunction.getNumberOfBuckets();
    }

    /**
     * Returns <code>true</code> if this iterator has one more element, otherwise it returns <code>false</code>. If an
     * error occurs while accessing the bucket file an {@link IllegalStateException} is thrown.
     */
    @Override
    public boolean hasNext() {
        if (readBuffer != null && readBuffer.remaining() != 0) {
            logger.debug("There are still elements in the readBuffer");
            return true;
        } else if (actualFile != null && actualFileOffset < actualFile.getFilledUpFromContentStart()) {
            logger.debug("The end of the actual file is not reached yet.");
            return true;
        } // Since version 0.2.23-SNAPSHOT all files are created, even if they don't contain elements
        else if (actualBucketId < numberOfBuckets - 1) {
            logger.debug("Not all files were read. Trying to open next file");
            try {
                this.handleFile();
            } catch (FileLockException e) {
                logger.error("Skipping file. Not all elements might have been iterated. {}", e);
            } catch (IOException e) {
                logger.error("Skipping file. Not all elements might have been iterated. {}", e);
            }
            return hasNext();
        }

        if (actualFile != null) {
            actualFile.close();
        }
        return false;
    }

    @Override
    public Data next() {
        try {
            while (handleFile() && readBuffer.remaining() == 0) {
                // if the readBuffer is empty after handling the file, than the next file will be opened
                handleReadBuffer();
            }

            if (readBuffer.remaining() == 0) {
                return null;
            }
            readBuffer.get(curDestBuffer);
            Data record = prototype.fromByteBuffer(ByteBuffer.wrap(curDestBuffer));
            countElementsRead++;
            return record;
        } catch (Exception e) {
            // TODO: handle exceptions properly
            e.printStackTrace();
        }

        if (actualFile != null) {
            actualFile.close();
        }
        return null;
    }

    /** fills the ReadBuffer from the HeaderIndexFile */
    private void handleReadBuffer() throws IOException {
        if (readBuffer.remaining() == 0) {
            readBuffer.clear();
            actualFile.read(actualFileOffset, readBuffer);
            actualFileOffset += readBuffer.limit();
            readBuffer.position(0);
        }
    }

    /**
     * @return true, if a file was set
     * @throws FileLockException
     * @throws IOException
     */
    private boolean handleFile() throws FileLockException, IOException {
        String filename = null;
        // if we open the first file
        if (readBuffer == null) {
            filename = gp.databaseDirectory + "/" + hashFunction.getFilename(actualBucketId);
            actualFile = new HeaderIndexFile<Data>(filename, 1, gp);
            readBuffer = ByteBuffer.allocate((int) actualFile.getChunkSize());
            readBuffer.clear();
            readBuffer.limit(0);
        } else if (readBuffer.remaining() == 0 && actualFileOffset >= actualFile.getFilledUpFromContentStart()) {
            actualFile.close();
            actualBucketId++;
            if (actualBucketId >= numberOfBuckets) {
                return false;
            }
            filename = gp.databaseDirectory + "/" + hashFunction.getFilename(actualBucketId);
            actualFile = new HeaderIndexFile<Data>(filename, 1, gp);
            actualFileOffset = 0;
            readBuffer.clear();
            readBuffer.limit(0);
        }
        return true;
    }

    /** Operation is <b>NOT</b> supported by this iterator. */
    @Override
    public void remove() {
        throw new UnsupportedOperationException("You can not delete records from DRUMS with an iterator.");
    }

    /** Closes this iterator. */
    @Override
    public void close() throws IOException {
        if (actualFile != null) {
            actualFile.close();
        }
    }
}
