package com.unister.semweb.sdrum.synchronizer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;


import com.unister.semweb.sdrum.GlobalParameters;
import com.unister.semweb.sdrum.bucket.Bucket;
import com.unister.semweb.sdrum.file.FileLockException;
import com.unister.semweb.sdrum.file.HeaderIndexFile;
import com.unister.semweb.sdrum.file.IndexForHeaderIndexFile;
import com.unister.semweb.sdrum.file.AbstractHeaderFile.AccessMode;
import com.unister.semweb.sdrum.storable.AbstractKVStorable;
import com.unister.semweb.sdrum.sync.SyncThread;

/**
 * Takes a list of {@link AbstractKVStorable} and synchronizes them with a file. A {@link Synchronizer} is instantiated
 * by a {@link SyncThread}.
 * The core assumption is that the list of {@link AbstractKVStorable} and the entries in the file are sorted ascended.
 * 
 * @author n.thieme, m.gleditzsch
 */
public class Synchronizer<Data extends AbstractKVStorable<Data>> {

    /** The file instance from which we receive a {@link FileChannel}. */
    protected String dataFilename;

    protected HeaderIndexFile<Data> dataFile;

    /** Number of entries that are read into memory. */
    protected int numberOfEntriesInOneChunk;

    /** Actual position in the file from which to read data. */
    protected long readOffset;

    /** Actual position in the file where to write the data. */
    protected long writeOffset;

    /** a buffer for writing always a bunch of elements */
    private ByteBuffer bufferedWriter;

    /** a buffer for reading a chunk (with elements) */
    private ByteBuffer bufferedReader;

    /** the header of the bucket, something like an index */
    private IndexForHeaderIndexFile header;

    /** the largest key in the actual chunk for writing */
    private long largestKeyInChunk;

    /** The number of entries that were added to the file. */
    private long numberOfInsertedEntries;

    /** The number of entries that were updated within the file. */
    private long numberOfUpdateEntries;

    LinkedList<byte[]> pendingElements = new LinkedList<byte[]>();
    private long filledUpToWhenStarted;

    // TODO 
    Data prototype;

    //TODO: comment
    private int elementSize;

    /**
     * This method constructs a {@link Synchronizer}. The name of the file were to write the elements to have to be
     * given.
     * 
     * @param dataFilename
     *            the file, where to store the {@link AbstractKVStorable}
     * @param Data
     *            prototype, a prototype of {@link AbstractKVStorable}. Will be further use as Flyweight-pattern.
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public Synchronizer(String dataFilename, Data prototype) {
        this.prototype = prototype;
        this.dataFilename = dataFilename;
        this.elementSize = prototype.getByteBufferSize();
        this.numberOfEntriesInOneChunk = (int) Math.floor(GlobalParameters.CHUNKSIZE / elementSize);
        this.bufferedWriter = ByteBuffer.allocate(numberOfEntriesInOneChunk * elementSize);
        this.bufferedReader = ByteBuffer.allocate(numberOfEntriesInOneChunk * elementSize);
    }

    /**
     * This method handles the given {@link AbstractKVStorable}s from a {@link Bucket}. It provides updates and inserts
     * of those objects knowing all already stored {@link AbstractKVStorable}s in the <code>dataFile</code>.
     * 
     * @param toAdd
     * @throws IOException
     */
    public void upsert(Data[] toAdd) throws IOException {
        try {
            /* Another thread can have access to this file in parallel. So we must wait to get exclusive access. */
            dataFile = new HeaderIndexFile<Data>(dataFilename, AccessMode.READ_WRITE, Integer.MAX_VALUE, elementSize);
            header = dataFile.getIndex(); // Pointer to the Index
        } catch (FileLockException e) {
            e.printStackTrace();
        }
        try {
            toAdd = (Data[])AbstractKVStorable.merge(toAdd);

            readOffset = 0;
            writeOffset = 0;
            filledUpToWhenStarted = dataFile.getFilledUpFromContentStart(); // need to remember how a many "old" bytes
                                                                            // are
            // in the file (will be overwritten)
            // We read one chunk from the disk. The initial chunk
            readNextChunkFromFile();

            // We take one AbstractKVStorable from the chunk. The chunk will be automatically incremented
            int indexOfToAdd = 0;
            byte[] dateFromDisk = getFromDisk();
            long dateFromDiskKey; // temporary key, performance boost, because only once a convert from byte -> long
            // if the date from disk is null, set key to 0
            dateFromDiskKey = (dateFromDisk != null ? ByteBuffer.wrap(dateFromDisk).getLong() : 0);

            // handle all AbstractKVStorable (update or insert)
            for (indexOfToAdd = 0; indexOfToAdd < toAdd.length && dateFromDisk != null;) {
                Data dateFromBucket = toAdd[indexOfToAdd];

                /* insert element from bucket */
                if (dateFromBucket.key < dateFromDiskKey) {
                    write(dateFromBucket.toByteBuffer().array(), dateFromBucket.key, false); // write date
                    indexOfToAdd++;// next dateFromBucket

                    // Incrementing the number of inserted entries.
                    numberOfInsertedEntries++;
                    continue;
                }

                /* merges element from bucket and element from disk */
                if (dateFromBucket.key == dateFromDiskKey) {
                    prototype.initFromByteBuffer(ByteBuffer.wrap(dateFromDisk));
                    Data newDate = prototype.merge(dateFromBucket);
                    
                    //TODO: think about update
                    write(newDate.toByteBuffer().array(), newDate.key, true); // write date
                    indexOfToAdd++; // next dateFromBucket
                    dateFromDisk = getFromDisk(); // get next date from disk
                    dateFromDiskKey = (dateFromDisk != null ? ByteBuffer.wrap(dateFromDisk).getLong() : 0);

                    // Incrementing the number of updated entries.
                    numberOfUpdateEntries++;
                    continue;
                }

                /* insert element from disk (pendingElements) */
                if (dateFromBucket.key > dateFromDiskKey) {
                    write(dateFromDisk, dateFromDiskKey, true); // write date
                    dateFromDisk = getFromDisk(); // get next date from disk
                    dateFromDiskKey = (dateFromDisk != null ? ByteBuffer.wrap(dateFromDisk).getLong() : 0);
                    continue;
                }
            }

            // end of the already stored elements, but there are still elements from bucket to insert
            if (dateFromDisk == null) {
                for (; indexOfToAdd < toAdd.length; indexOfToAdd++) {
                    write(toAdd[indexOfToAdd].toByteBuffer().array(), toAdd[indexOfToAdd].key, false);

                    // Incrementing the number of inserted entries.
                    numberOfInsertedEntries++;
                }
            }

            // all entries from bucket were added, but there are still pending elements .
            while (dateFromDisk != null) {
                this.write(dateFromDisk, dateFromDiskKey, true);
                dateFromDisk = getFromDisk();
                dateFromDiskKey = (dateFromDisk != null ? ByteBuffer.wrap(dateFromDisk).getLong() : 0);
            }

            // write the remaining elements from the bufferedWriter to the disk
            this.writeBuffer();
            int lastChunkId = dataFile.getChunkIndex(writeOffset + bufferedWriter.position());
            this.header.setLargestKey(lastChunkId, largestKeyInChunk);
        } finally {
            // close the file
            dataFile.close();
        }
    }

    /**
     * writes the given {@link byte[]} to the given {@link FileChannel}. This method improves performance by 30%,
     * because it is not needed anymore to create all AbstractKVStorable from byte-arrays. The given byte-array contains
     * only one {@link AbstractKVStorable}.
     * 
     * @param data
     * @param channel
     * @throws IOException
     */
    protected boolean write(byte[] newData, long key, boolean alreadyExist) throws IOException {
        // If the link data is invalid.
        if (key == 0) {
            return false;
        }
        
        // if the last readChunk was full
        ByteBuffer toAdd = ByteBuffer.wrap(newData);
        long positionOfToAddInFile = writeOffset + bufferedWriter.position();
        bufferedWriter.put(toAdd);
        largestKeyInChunk = key; // elements are stored ordered so we can easily remember the largest key

        int chunkId = dataFile.getChunkIndex(positionOfToAddInFile);
        header.setLargestKey(chunkId, largestKeyInChunk);

        if (bufferedWriter.remaining() == 0) {
            writeBuffer();
        }
        return true;
    }

    /**
     * writes the remaining bytes in bufferedWriter to the given FileChannel
     * 
     * @param FileChannel
     * @throws IOException
     */
    private void writeBuffer() throws IOException {
        bufferedWriter.flip();
        dataFile.write(writeOffset, bufferedWriter);
        writeOffset += bufferedWriter.limit();
        bufferedWriter.clear();
        readNextChunkFromFile(); // before overwriting the next bucket. Read it.
    }

    /**
     * Gets the next {@link AbstractKVStorable} from disk or from the cache. If the end of the cache is reached a new
     * read will be made from the file.
     * 
     * @return
     * @throws IOException
     */
    private byte[] getFromDisk() throws IOException {
        if (!pendingElements.isEmpty()) {
            return pendingElements.pop();
        }
        return null;
    }

    /**
     * Reads one chunk from the file. It is possible that the chunk is not complete because the end of the file is
     * reached. In this case the maximum number of {@link AbstractKVStorable} will be retrieved.
     * 
     * @return a chunk from the file
     * @throws IOException
     */
    private void readNextChunkFromFile() throws IOException {
        if (readOffset >= filledUpToWhenStarted) {
            return;
        }

        bufferedReader.clear();
        dataFile.read(readOffset, bufferedReader);
        bufferedReader.position(0);

        if (bufferedReader.limit() > 0) {
            readOffset += bufferedReader.limit();
            int numberOfEntries = bufferedReader.limit() / elementSize;
            for (int i = 0; i < numberOfEntries; i++) {
                byte[] dst = new byte[elementSize];
                bufferedReader.get(dst);
                pendingElements.add(dst);
            }
        }
    }

    /** Closes, if not yet closed, the dataFile */
    public void close() {
        if (dataFile != null && dataFile.isOpen()) {
            dataFile.close();
        }
    }

    /* Measurement methods */
    /** Returns the number of entries that were inserted. */
    public long getNumberOfInsertedEntries() {
        return numberOfInsertedEntries;
    }

    /** Returns the number of entries that were updated in the file. */
    public long getNumberOfUpdatedEntries() {
        return numberOfUpdateEntries;
    }
}