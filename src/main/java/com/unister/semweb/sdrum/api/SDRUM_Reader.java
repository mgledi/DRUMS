package com.unister.semweb.sdrum.api;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.unister.semweb.sdrum.file.FileLockException;
import com.unister.semweb.sdrum.file.HeaderIndexFile;
import com.unister.semweb.sdrum.storable.AbstractKVStorable;
import com.unister.semweb.sdrum.utils.KeyUtils;

/**
 * This class represents an efficient direct access reader. It holds all files opened for reading. Only use this Reader,
 * when there are no write-operations during reading. The files will be locked.
 * 
 * @author m.gleditzsch
 */
public class SDRUM_Reader<Data extends AbstractKVStorable<Data>> {
    static Logger logger = LoggerFactory.getLogger(SDRUM_Reader.class);

    /** Marks if files are opend. Is set to avoid null-pointer exceptions */
    private boolean filesAreOpend = false;

    /** The Instance of this singelton */
    @SuppressWarnings("rawtypes")
    public static SDRUM_Reader INSTANCE;

    /** An array containing all used files. All files are opened when instantiating for performance reasons. */
    private HeaderIndexFile<Data>[] files;

    /** A pointer to the used SDRUM */
    private SDRUM<Data> sdrum;

    /** An array which knows for each file in <code>files</code> the number of elements. (performance) */
    int[] cumulativeElementsPerFile;

    /** a prototype of Data */
    private Data prototype;

    private int numberOfBuckets;
    private int elementSize;
    private int elementsPerChunk;

    /** temporarily used destination buffer */
    private ByteBuffer destBuffer;

    private SDRUM_Reader(SDRUM<Data> sdrum) throws FileLockException, IOException {
        this.sdrum = sdrum;
        numberOfBuckets = sdrum.getHashFunction().getNumberOfBuckets();
        elementSize = sdrum.getElementSize();
        prototype = sdrum.getPrototype();
        openFiles();
    }

    /** Instantiates a new SDRUM_Reader */
    public static <Data extends AbstractKVStorable<Data>> void instantiate(SDRUM<Data> sdrum)
            throws FileLockException, IOException {
        if (INSTANCE == null) {
            INSTANCE = new SDRUM_Reader<Data>(sdrum);
        }
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
        String path = sdrum.getDatabaseDirectory();
        for (int i = 0; i < numberOfBuckets; i++) {
            String filename = path + sdrum.getHashFunction().getFilename(i);
            if (!new File(filename).exists()) {
                cumulativeElementsPerFile[i] = 0;
            } else {
                lastfile = i;
                files[i] = new HeaderIndexFile<Data>(filename, 10);
                cumulativeElementsPerFile[i] = (int) (files[i].getFilledUpFromContentStart() / elementSize);
            }
            if (i > 0) {
                cumulativeElementsPerFile[i] += cumulativeElementsPerFile[i - 1];
            }
        }

        elementsPerChunk = files[lastfile].getChunkSize() / elementSize;
        destBuffer = ByteBuffer.allocate(files[lastfile].getChunkSize());
        filesAreOpend = true;
    }

    /**
     * Returns all elements between lowerKey and upperKey
     * this function is still BUGGY
     */
    public List<Data> getRange(byte[] lowerKey, byte[] upperKey) throws IOException {
        if (!filesAreOpend) {
            throw new IOException("The files are not opened yet. Use openFiles() to open all files.");
        }
        // estimate bounds
        int lowerBucket = sdrum.getHashFunction().getBucketId(lowerKey);
        int upperBucket = sdrum.getHashFunction().getBucketId(lowerKey);
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
                    Data record = prototype.fromByteBuffer(ByteBuffer.wrap(tmpB));
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
        IntObjectOpenHashMap<ArrayList<byte[]>> bucketKeyMapping = sdrum.getBucketKeyMapping(keys);
        for (IntObjectCursor<ArrayList<byte[]>> entry : bucketKeyMapping) {
            ArrayList<byte[]> keyList = entry.value;
            result.addAll(sdrum.searchForData(files[entry.key], keyList.toArray(new byte[keyList.size()][])));
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

    /** Closes the Instance of the SDRUM_Reader */
    public static void close() {
        if (INSTANCE != null) {
            INSTANCE.closeFiles();
            INSTANCE = null;
        }
    }
}
