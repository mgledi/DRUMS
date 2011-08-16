package com.unister.semweb.superfast.storage.api;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.unister.semweb.superfast.storage.bucket.hashfunction.AbstractHashFunction;
import com.unister.semweb.superfast.storage.file.AbstractHeaderFile.AccessMode;
import com.unister.semweb.superfast.storage.file.FileLockException;
import com.unister.semweb.superfast.storage.file.HeaderIndexFile;
import com.unister.semweb.superfast.storage.storable.AbstractKVStorable;

/** Make it possible to iterate over all elements of all buckets. */
public class SDrumIterator<Data extends AbstractKVStorable<Data>> implements Iterator<Data>, Closeable {
    private static final Logger log = LoggerFactory.getLogger(SDrumIterator.class);

    /**
     * The hash function that determines the bucket distribution and holds the mapping between the bucket id and the
     * filename of the bucket:
     */
     private AbstractHashFunction hashFunction;
    private Data prototype;
    private int sizeOfDate;
    private HeaderIndexFile<Data> actualFile;
    private long currentOffset = 0;
    private int currentBucket = 0;
    private int numberOfBuckets = 0;

    /** Initialises the iterator with the hash function. */
    public SDrumIterator(AbstractHashFunction hashFunction, Data prototype) {
        this.prototype = prototype;
        this.sizeOfDate = prototype.getByteBufferSize();
        this.hashFunction = hashFunction;
        numberOfBuckets = hashFunction.getNumberOfBuckets();
    }

    /**
     * Returns <code>true</code> if this iterator has one more element, otherwise it returns <code>false</code>. If an
     * error occurs while accessing the bucket file an {@link IllegalStateException} is thrown.
     */
    @Override
    public boolean hasNext() {
        if (currentOffset < actualFile.getFilledUpFromContentStart()) {
            return true;
        }

        currentOffset = 0;
        actualFile.close();
        currentBucket++;
        
        boolean oneFound = false;
        while (currentBucket < numberOfBuckets && !oneFound) {
            String filename = hashFunction.getFilename(currentBucket);
            try {
            actualFile = new HeaderIndexFile<Data>(filename, AccessMode.READ_ONLY, 100, sizeOfDate);
            oneFound = true;
            } catch(FileLockException ex) {
                log.error("Could not lock database file {}. I will jump over this file", filename);
            } catch(IOException ex) {
                log.error("An IO error occurred while accessing the database file {}. I will jump over.", filename);
            }
        }
        return false;
    }

    @Override
    public Data next() {
        ByteBuffer dataBuffer = ByteBuffer.allocate(sizeOfDate);
        
        try {
        actualFile.read(currentOffset, dataBuffer);
        } catch(IOException ex) {
            throw new IllegalStateException(ex);
        }
        dataBuffer.flip();

        Data date = prototype.fromByteBuffer(dataBuffer);
        currentOffset = currentOffset + sizeOfDate;
        return date;
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
