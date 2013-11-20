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
package com.unister.semweb.drums.file;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

import com.unister.semweb.drums.GlobalParameters;
import com.unister.semweb.drums.storable.AbstractKVStorable;
import com.unister.semweb.drums.utils.KeyUtils;

/**
 * This class represents a file, which contains a bunch of datasets. The file also contains a header with some
 * informations and an {@link IndexForHeaderIndexFile}. This class is for managing and handling equal sized storable
 * elements. So be very careful if this precondition is not fulfilled.<br>
 * <br>
 * Header structure:<br/>
 * <br/>
 * <code>
 * +-----------+--------------+---------------+---------------+-------------+---------+<br/>
 * | FILE SIZE | FILLED UP TO | Closed Softly | ReadChunkSize | ElementSize | KeySize |<br/>
 * | 8 bytes . | 8 bytes .... | 1 bytes ..... | 4 bytes ..... | 4 bytes ... | 4 bytes |<br/>
 * +-----------+--------------+---------------+---------------+-------------+---------+<br/>
 * </code> = 1024 bytes (to have enough space for more values)<br/>
 * <br/>
 * To use this class correctly, have a look at the following methods: <li>read(long offset, ByteBuffer destBuffer) <li>
 * write(long offset, ByteBuffer sourceBuffer) <li>append(ByteBuffer sourceBuffer) <li>dbfile.getFilledUpToExclHeader()
 * <br>
 * <br>
 * The file enlarges automatically. Deletes (writing empty byte[] to a specific position) are leading to fragmentation.
 * The actual version don't have a fragmentation-handling. Example code for reading the whole file:
 * 
 * <pre>
 *  ...
 *  DBFile dbfile = new DBFile(filename, mode, retries);
 *  ByteBuffer readBuffer = ByteBuffer.allocate(blockSize);
 *  long offset = 0;
 *  while(offset < dbfile.getFilledUpFromContentStart()) {
 *      read(offset, readBuffer);
 *      // do something with the readBuffer
 *      ...
 *      offset += readBuffer.limit();
 *  }
 *  ...
 * </pre>
 * 
 * @author Martin Nettling
 */
public class HeaderIndexFile<Data extends AbstractKVStorable> extends AbstractHeaderFile implements IWritableIndexFile {

    /** the size of the index in bytes */
    protected static final long MAX_INDEX_SIZE_IN_BYTES = 512 * 1024; // 512 kb

    /** the byte offset, where the index starts */
    protected static final int INDEX_OFFSET = HEADER_SIZE; // index starts after the header

    /** the time to wait for retry the access, if the file was locked */
    protected static final int RETRY_CONNECT_WAITTIME = 250;

    /** if true, the file enlarges automatically if <code>size</code> is reached */
    protected static final boolean AUTO_ENLARGE = true;

    /** the size of a stored element in bytes */
    protected int elementSize;// part of the header

    protected int keySize;

    /** shows if the file was closed correctly */
    private byte closedSoftly = 0; // PART OF HEADER (1 bytes)

    /** in bytes */
    protected int chunkSize;

    /** a constant size, by which the file will be resized in byte */
    protected int incrementSize;

    /** a mappedByteBuffer to the index-region. So changes can be made directly */
    protected MappedByteBuffer indexBuffer;

    /** the number of elements, which can be stored in the index */
    protected int indexSize;

    /** the size of the index in bytes */
    protected long indexSizeInBytes;

    /** A pseudo-index (not each element is indexed, but the chunks where they belong to */
    protected IndexForHeaderIndexFile<Data> index;

    /** A pointer to the GlobalParameters used by this DRUMS */
    protected GlobalParameters<Data> gp;

    /**
     * This constructor instantiates a new {@link HeaderIndexFile} with the given <code>fileName</code> in the given
     * {@link AccessMode}.
     * 
     * @param <b>String</b> fileName, the filename of the underlying OSfile.
     * @param <b>AccessMode</b> mode, the mode the file should be accessed. READ_ONLY or READ_WRITE
     * @param <b>int</b> max_retries_connect, the number of retries to open a channel, if the file is locked
     * @param TODO
     * @throws FileLockException
     *             if the <code>max_retries_connect</code> is exceeded
     * @throws IOException
     *             if another error with the fileaccess occured
     */
    public HeaderIndexFile(String fileName, AccessMode mode, int max_retries_connect, GlobalParameters<Data> gp)
            throws FileLockException, IOException {
        this.gp = gp;
        this.incrementSize = gp.INITIAL_INCREMENT_SIZE;
        this.elementSize = gp.elementSize;
        this.keySize = gp.keySize;
        this.osFile = new File(fileName);
        this.mode = mode;
        this.max_retries_connect = max_retries_connect;
        this.init();
        if (closedSoftly == 0) {
            logger.warn("File {} was not closed correctly and might be corrupted", osFile.getName());
        }
    }

    /**
     * This constructor instantiates a new {@link HeaderIndexFile} with the given <code>fileName</code> in the given
     * {@link AccessMode}. This is a weak constructor and therefore you can only have read-access.
     * 
     * @param <b>String</b> fileName, the filename of the underlying OSfile.
     * @param <b>int</b> max_retries_connect, the number of retries to open a channel, if the file is locked
     * @throws FileLockException
     *             if the <code>max_retries_connect</code> is exceeded
     * @throws IOException
     *             if another error with the fileaccess occured
     */
    public HeaderIndexFile(String fileName, int max_retries_connect, GlobalParameters<Data> gp)
            throws FileLockException, IOException {
        this.osFile = new File(fileName);
        this.max_retries_connect = max_retries_connect;
        this.gp = gp;
        this.init();
        if (closedSoftly == 0) {
            logger.warn("File {} was not closed correctly and might be corrupted", osFile.getName());
        }
    }

    protected void init() throws FileLockException, IOException {
        this.contentStart = HEADER_SIZE + MAX_INDEX_SIZE_IN_BYTES;
        if (!osFile.exists()) {
            logger.debug("File {} not found. Initialise new File.", osFile.getAbsolutePath());
            this.createFile();
        } else {
            logger.debug("File {} exists. Try to open it.", osFile.getAbsolutePath());
            openChannel();
        }
        this.contentEnd = size;

        if (chunkSize % elementSize != 0) {
            logger.warn(
                    "The readChunkSize ({}) is not a multiple of elementsize ({}). This might lead to a wrong index",
                    chunkSize, elementSize);
        }
    }

    /**
     * Create a {@link MappedByteBuffer} mapping the given region. The size of the HEADER will be respected
     * automatically. <br>
     * <br>
     * WARNING: This method is for pro-use only.
     */
    public MappedByteBuffer map(long offset, int length) throws IOException {
        offset += contentStart;
        if (mode == AccessMode.READ_ONLY) {
            return channel.map(FileChannel.MapMode.READ_ONLY, offset, length);
        } else {
            return channel.map(FileChannel.MapMode.READ_WRITE, offset, length);
        }
    }

    /**
     * writes the bytes from the given ByteBuffer to the file beginning at offset. The size of the HEADER will be
     * respected automatically.
     * 
     * @param long offset
     * @param ByteBuffer
     *            sourceBuffer
     * @throws IOException
     */
    @Override
    public void write(long offset, ByteBuffer sourceBuffer) throws IOException {
        offset += contentStart;
        sourceBuffer.position(0);

        long newFilePosition = offset + sourceBuffer.limit();
        if (newFilePosition > contentEnd) {
            logger.debug("Filesize exceeded (contendEnd: {},size: {})", contentEnd, size);
            if (AUTO_ENLARGE) {
                // if the incrementSize is not large enough
                // if (offset + sourceBuffer.limit() > contentEnd + incrementSize) {
                // incrementSize = (int) (offset + sourceBuffer.limit() - contentEnd + incrementSize);
                // }
                this.enlargeFile(newFilePosition - contentEnd);
            } else {
                throw new IOException("Filesize exceeded (" + size
                        + ") and automatic enlargement is disabled. You have to enlarge file manually.");
            }
        }

        if (offset + sourceBuffer.limit() > filledUpTo) {
            filledUpTo = offset + sourceBuffer.limit();
        }
        channel.write(sourceBuffer, offset);
        writeHeader();
    }

    /**
     * writes the bytes from the given Byte-array to the file beginning at offset. The size of the HEADER will be
     * respected automatically.
     * 
     * @param long offset
     * @param byte[] sourceBuffer
     * @throws IOException
     */
    @Override
    public void write(long offset, byte[] sourceBuffer) throws IOException {
        write(offset, ByteBuffer.wrap(sourceBuffer));
    }

    /**
     * appends the given sourceBuffer to the file
     * 
     * @param byte[] sourceBuffer
     * @throws IOException
     */
    @Override
    public long append(byte[] sourceBuffer) throws IOException {
        return append(ByteBuffer.wrap(sourceBuffer));
    }

    /**
     * appends the given sourceBuffer to the file
     * 
     * @param ByteBuffer
     *            sourceBuffer
     * @throws IOException
     */
    @Override
    public long append(ByteBuffer sourceBuffer) throws IOException {
        long positionInFile = filledUpTo - contentStart;
        write(positionInFile, sourceBuffer);
        return positionInFile;
    }

    /**
     * Reads x bytes from the file to the given ByteBuffer (position was set to zero), where x is the minimum of the
     * capacity of the buffer and the remaining written bytes in the file. The size of the HEADER will be respected
     * automatically. The position is set to the number of read bytes.
     * 
     * @param long offset
     * @param ByteBuffer
     *            destBuffer
     * @throws IOException
     */
    @Override
    public int read(long offset, ByteBuffer destBuffer) throws IOException {
        offset += contentStart;
        destBuffer.clear();
        int length = destBuffer.capacity();
        if (offset > filledUpTo) {
            String errorMessage = "Tried to read data beginning at " + offset + " in File (max=" + filledUpTo + ") "
                    + osFile.getName();
            logger.debug(errorMessage);
            throw new IOException(errorMessage);
        } else if (offset + length > filledUpTo) {
            logger.debug("Tried to read data over {} in File " + osFile.getName() + ". Limit will be set to {}",
                    filledUpTo, destBuffer.limit());
            destBuffer.limit((int) (filledUpTo - offset));
        }
        offset = checkRegions(offset, destBuffer.limit());
        int readBytes = channel.read(destBuffer, offset);
        return readBytes;
    }

    /**
     * Reads x bytes from the file to the given ByteBuffer, where x is the minimum of the capacity of the buffer and the
     * remaining written bytes in the file. The size of the HEADER will be respected automatically.
     * 
     * @param long offset
     * @param ByteBuffer
     *            destBuffer
     * @throws IOException
     */
    @Override
    public int read(long offset, byte[] destBuffer) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(destBuffer);
        return read(offset, buffer);
    }

    /** this function calculates all relevant informations for the index */
    public void calcIndexInformations() {
        indexSize = (int) Math.floor(MAX_INDEX_SIZE_IN_BYTES / keySize);
        indexSizeInBytes = indexSize * keySize;
    }

    /**
     * opens the {@link RandomAccessFile} and the corresponding {@link FileChannel}. Optionally reads the header. and
     * the index
     * 
     * @param boolean - should the header be read
     * @param boolean - should the index be read
     * @throws IOException
     */
    protected void openChannel(boolean readHeader, boolean readIndex) throws FileLockException, IOException {
        super.openChannel(readHeader);
        calcIndexInformations();
        if (mode == AccessMode.READ_ONLY) {
            indexBuffer = channel.map(FileChannel.MapMode.READ_ONLY, INDEX_OFFSET, indexSizeInBytes);
        } else {
            indexBuffer = channel.map(FileChannel.MapMode.READ_WRITE, INDEX_OFFSET, indexSizeInBytes);
        }

        if (readIndex) {
            readIndex();
        }
    }

    /**
     * opens the {@link RandomAccessFile} and the corresponding {@link FileChannel}. Optionally reads the header. Reads
     * the index. This may overwrite previous set parameters.
     * 
     * @param boolean - should the header be read
     * @throws IOException
     */
    protected void openChannel(boolean readHeader) throws FileLockException, IOException {
        openChannel(false, true);
    }

    /**
     * opens the {@link RandomAccessFile} and the corresponding {@link FileChannel}. Reads the header and the index.
     * This may overwrite previous set parameters.
     * 
     * @throws IOException
     */
    public void openChannel() throws FileLockException, IOException {
        openChannel(true, true);
    }

    protected void createFile() throws FileLockException, IOException {
        size = gp.INITIAL_FILE_SIZE;
        filledUpTo = contentStart;
        chunkSize = (int) gp.FILE_CHUNK_SIZE;
        openChannel(false, false);
        setSoftlyClosed(true);
        // have to reset the informations cause in openchannel the empty header was read
        accessFile.setLength(gp.INITIAL_FILE_SIZE);
        writeHeader();
        readIndex(); // index should be empty
    }

    public void close() {
        super.close();
        if (this.index != null) {
            index = null;
        }
        if (indexBuffer != null) {
            indexBuffer.force();
            indexBuffer = null;
        }
    }

    public long getFreeSpace() {
        return size - filledUpTo;
    }

    /** reads and instantiates the index from the <code>indexBuffer</code> */
    public void readIndex() {
        indexBuffer.rewind();
        index = new IndexForHeaderIndexFile<Data>(indexSize, keySize, chunkSize, indexBuffer);
    }

    protected void readHeader() {
        headerBuffer.rewind();
        size = headerBuffer.getLong();
        filledUpTo = headerBuffer.getLong();
        closedSoftly = headerBuffer.get();
        chunkSize = headerBuffer.getInt();
        elementSize = headerBuffer.getInt();
        keySize = headerBuffer.getInt();
    }

    protected void writeHeader() {
        headerBuffer.rewind();
        headerBuffer.putLong(size);
        headerBuffer.putLong(filledUpTo);
        headerBuffer.put(closedSoftly);
        headerBuffer.putInt(chunkSize);
        headerBuffer.putInt(elementSize);
        headerBuffer.putInt(keySize);
    }

    @Override
    public void enlargeFile(long atLeastTargetSize) throws IOException {
        long numberOfEnlargmentSegments = (long) Math.ceil((double) atLeastTargetSize / incrementSize);
        // long numberOfEnlargmentSegments = MathUtils.divideLongsWithCeiling(atLeastTargetSize, incrementSize);
        // size += (numberOfEnlargmentSegments * incrementSize);
        size += (incrementSize * numberOfEnlargmentSegments);
        logger.debug("Enlarge filesize of {} to {}", osFile, size);
        contentEnd = size;
        accessFile.setLength(size);

        // TODO check buffer size
        // if there are more chunks than the index can contain
        if ((size - contentStart) / chunkSize > indexSize) {
            logger.error("File-Enlargement not possible. The index becomes too large.");
            throw new IOException("File-Enlargement not possible. The index becomes too large.");
        }
        calcIndexInformations();
    }

    /**
     * returns the instantiated index
     * 
     * @return {@link IndexForHeaderIndexFile}
     */
    public IndexForHeaderIndexFile<Data> getIndex() {
        return index;
    }

    /**
     * This method checks, if the keys of the all inserted elements are incrementing continuously.
     * 
     * @return boolean
     * @throws IOException
     */
    public boolean isConsistent() throws IOException {
        byte[] b = new byte[elementSize];
        long offset = 0;
        byte[] oldKey = null;
        int i = 0;
        while (offset < this.getFilledUpFromContentStart()) {
            this.read(offset, ByteBuffer.wrap(b));
            byte[] key = Arrays.copyOfRange(b, 0, keySize);
            if (oldKey != null) {
                if (KeyUtils.compareKey(key, oldKey) != 1) {
                    logger.error("File is not consistent at record {}. {} not larger than {}", new Object[] { i,
                            KeyUtils.transform(key), KeyUtils.transform(oldKey) });
                    return false;
                }
            }
            oldKey = Arrays.copyOf(key, keySize);
            offset += elementSize;
            i++;
        }
        return true;
    }

    /**
     * This method checks, if the data is consistent with the index.
     * 
     * @return boolean
     * @throws IOException
     */
    public boolean isConsitentWithIndex() throws IOException {
        byte[] b = new byte[keySize];
        long offset = 0;
        int maxChunk = getChunkIndex(getFilledUpFromContentStart());
        boolean isConsistent = true;
        for (int i = 1; i <= maxChunk; i++) {
            offset = i * getChunkSize() - elementSize;
            read(offset, ByteBuffer.wrap(b));
            if (KeyUtils.compareKey(getIndex().maxKeyPerChunk[i - 1], b) != 0) {
                logger.error("Index is not consistent to data. Expected {}, but found {}.",
                        Arrays.toString(getIndex().maxKeyPerChunk[i - 1]), Arrays.toString(b));
                isConsistent = false;
            }
        }
        return isConsistent;
    }

    /**
     * This method repairs the index. It runs over all chunks and writes the largest element into the
     * {@link IndexForHeaderIndexFile}.
     * 
     * @throws IOException
     */
    // public void repairIndex() throws IOException {
    // byte[] b = new byte[keySize];
    // long offset = 0;
    // int maxChunk = getChunkIndex(getFilledUpFromContentStart());
    // for (int i = 1; i <= maxChunk; i++) {
    // offset = i * getChunkSize() - elementSize;
    // read(offset, ByteBuffer.wrap(b));
    // getIndex().setLargestKey(i - 1, b);
    // if (KeyUtils.compareKey(getIndex().maxKeyPerChunk[i - 1], b) != 0) {
    // logger.info("Index is not consistent to data. Expected {}, but found {}.",
    // Arrays.toString(getIndex().maxKeyPerChunk[i - 1]), Arrays.toString(b));
    // }
    // }
    // }

    public void repairIndex() throws IOException {
        byte[] currentLargestKey = new byte[keySize];
        int maxChunk = getChunkIndex(getFilledUpFromContentStart());
        for (int currentChunkId = 0; currentChunkId <= maxChunk; currentChunkId++) {
            long currentOffset = (currentChunkId + 1) * getChunkSize() - elementSize;
            if (currentOffset < getFilledUpFromContentStart()) {
                read(currentOffset, currentLargestKey);
            } else {
                read(getFilledUpFromContentStart() - elementSize, currentLargestKey);
            }
            getIndex().setLargestKey(currentChunkId, currentLargestKey);
        }
    }

    /** sets the start of filledUpTo to zero */
    public void clear() {
        this.filledUpTo = 0;
        this.contentStart = HEADER_SIZE + MAX_INDEX_SIZE_IN_BYTES;
        this.filledUpTo = contentStart;
    }

    /**
     * calculates the index of the belonging readChunk in the files content
     * 
     * @param long offset, the byteOffset in the content
     * @return
     */
    public int getChunkIndex(long offset) {
        return (int) offset / this.chunkSize;
    }

    /** returns the size of one element. We assume that all elements are equal sized */
    public int getElementSize() {
        return elementSize;
    }

    /** returns the size in bytes of a chunk */
    public int getChunkSize() {
        return chunkSize;
    }

    /** true, if the file was closed softly */
    public boolean isSoftlyClosed() {
        return closedSoftly == 1;
    }

    protected void setSoftlyClosed(boolean value) {
        if (headerBuffer == null) {
            return;
        }
        closedSoftly = (byte) (value ? 1 : 0);
        this.writeHeader();
    }
}
