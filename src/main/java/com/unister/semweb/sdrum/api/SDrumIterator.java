package com.unister.semweb.sdrum.api;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;

import com.unister.semweb.sdrum.bucket.hashfunction.AbstractHashFunction;
import com.unister.semweb.sdrum.bucket.hashfunction.RangeHashFunction;
import com.unister.semweb.sdrum.file.FileLockException;
import com.unister.semweb.sdrum.file.HeaderIndexFile;
import com.unister.semweb.sdrum.storable.AbstractKVStorable;
import com.unister.semweb.sdrum.storable.DummyKVStorable;

/**
 * This class instantiates an Read-Only-Iterator for a given SDRUM
 * 
 * @author m.gleditzsch
 * @param <Data>
 */
public class SDrumIterator<Data extends AbstractKVStorable<Data>> implements Iterator<Data>, Closeable {
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
    byte[] actualDestination;

    /** for fast access, the number of buckets */
    private int numberOfBuckets = 0;

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
        this.actualDestination = new byte[elementSize];
        numberOfBuckets = hashFunction.getNumberOfBuckets();
    }

    /**
     * Returns <code>true</code> if this iterator has one more element, otherwise it returns <code>false</code>. If an
     * error occurs while accessing the bucket file an {@link IllegalStateException} is thrown.
     */
    @Override
    public boolean hasNext() {
        if (readBuffer != null && readBuffer.remaining() != 0 ) {
            return true;
        } else if (actualFile != null && actualFile.getFilledUpFromContentStart() > actualFileOffset) {
            return true;
        } else if (actualBucketId < numberOfBuckets-1) { // TODO: this is weak, there could be empty buckets 
            return true;
        }
        return false;
    }

    @Override
    public Data next() {
        try {
            if(!handleFile()) {
                return null;
            }
            handleReadBuffer();
            if(readBuffer.remaining() == 0) {
                return null;
            }
            readBuffer.get(actualDestination);
            Data d = prototype.fromByteBuffer(ByteBuffer.wrap(actualDestination));
//            d.initFromByteBuffer(ByteBuffer.wrap(actualDestination));
            return d;
        } catch (FileLockException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    private void handleReadBuffer() throws IOException {
        if (readBuffer.remaining() == 0) {
            actualFile.read(actualFileOffset, readBuffer);
            actualFileOffset += readBuffer.limit();
            readBuffer.rewind();
        }
    }

    /**
     * 
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
            readBuffer.limit(0);
            System.out.println(filename);
        } else if (readBuffer.remaining() == 0 && actualFile.getFilledUpFromContentStart() >= actualFileOffset) {
            actualFile.close();
            System.out.println("Changing file");
            actualBucketId++;
            if(actualBucketId >= numberOfBuckets) {
                return false;
            }
            filename = directory + "/" + hashFunction.getFilename(actualBucketId);
            actualFile = new HeaderIndexFile<Data>(filename, 1);
            actualFileOffset = 0;
            readBuffer.limit(0);
            System.out.println(filename);
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
