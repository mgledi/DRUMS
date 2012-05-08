package com.unister.semweb.sdrum.api;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.unister.semweb.sdrum.bucket.hashfunction.AbstractHashFunction;
import com.unister.semweb.sdrum.file.FileLockException;
import com.unister.semweb.sdrum.file.HeaderIndexFile;
import com.unister.semweb.sdrum.storable.AbstractKVStorable;

/**
 * This class instantiates an Read-Only-Iterator for a given SDRUM
 * 
 * @author m.gleditzsch
 * @param <Data>
 */
public class SDrumIterator<Data extends AbstractKVStorable<Data>> implements Iterator<Data>, Closeable {
    static Logger logger = LoggerFactory.getLogger(SDrumIterator.class);
    /** The hash function. Maps an element to a bucket. */
    private AbstractHashFunction hashFunction;

    /** a prototype of the elements to handle */
    private Data prototype;

    /** the size of one element, for fast access */
    private int elementSize;

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

    private int countElementsRead = 0;

    private String directory;

    /**
     * Initialises the iterator with the hash function.
     * 
     * @param hashFunction
     * @param prototype
     */
    public SDrumIterator(String directory, AbstractHashFunction hashFunction, Data prototype) {
        this.directory = directory;
        this.prototype = prototype;
        this.elementSize = prototype.getByteBufferSize();
        this.hashFunction = hashFunction;
        this.curDestBuffer = new byte[elementSize];
        numberOfBuckets = hashFunction.getNumberOfBuckets();
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
                handleReadBuffer();
            }

            if (readBuffer.remaining() == 0) {
                return null;
            }
            readBuffer.get(curDestBuffer);
            Data d = prototype.fromByteBuffer(ByteBuffer.wrap(curDestBuffer));
            countElementsRead++;
            return d;
        } catch (FileLockException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
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
            filename = directory + "/" + hashFunction.getFilename(actualBucketId);
            actualFile = new HeaderIndexFile<Data>(filename, 1);
            readBuffer = ByteBuffer.allocate(actualFile.getChunkSize());
            readBuffer.clear();
            readBuffer.flip();
        } else if (readBuffer.remaining() == 0 && actualFileOffset >= actualFile.getFilledUpFromContentStart()) {
            actualFile.close();
            actualBucketId++;
            if (actualBucketId >= numberOfBuckets) {
                return false;
            }
            filename = directory + "/" + hashFunction.getFilename(actualBucketId);
            actualFile = new HeaderIndexFile<Data>(filename, 1);
            actualFileOffset = 0;
            readBuffer.clear();
            readBuffer.flip();
        }
        return true;
    }

    /**
     * Operation is <b>NOT</b> supported by this iterator.
     */
    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    /** Closes this iterator. */
    @Override
    public void close() throws IOException {
        if (actualFile != null) {
            actualFile.close();
        }
    }
}
