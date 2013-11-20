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
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Header structure:<br/>
 * <br/>
 * <code>
 * +-----------+--------------+---------------+---------------+<br/>
 * | FILE SIZE | FILLED UP TO | ............. | ............. |<br/>
 * | 8 bytes . | 8 bytes .... | ............. | ............. |<br/>
 * +-----------+--------------+---------------+---------------+<br/>
 * </code> = 1024 bytes (to have enough space for more values)<br/>
 * <br/>
 * To use this class correctly, have a look at the following methods: <li>read(long offset, ByteBuffer destBuffer) <li>
 * write(long offset, ByteBuffer sourceBuffer) <li>append(ByteBuffer sourceBuffer) <li>dbfile.getFilledUpToExclHeader()
 * <br>
 * <br>
 * 
 * @author Martin Nettling
 */
public abstract class AbstractHeaderFile {
    Logger logger = LoggerFactory.getLogger(this.getClass());

    /** the Default size of a file. (16MB) */
    public final int DEFAULT_SIZE = 16 * 1024 * 1024;
    /** the time to wait for a retry if a file is locked */
    public final int RETRY_CONNECT_WAITTIME = 250;

    /** The possible AccessModes. READ_ONLY and READ_WRITE */
    public enum AccessMode {
        READ_ONLY, READ_WRITE
    };

    /** the size of the header */
    public static final int HEADER_SIZE = 1024;

    /** the file to access */
    public File osFile; // TODO:private

    /** the file to access */
    protected RandomAccessFile accessFile;

    /** the local channel from the <code>accessFile</code> */
    protected FileChannel channel;

    /** a mappedByteBuffer to the header-regions. So changes can be made directly */
    protected MappedByteBuffer headerBuffer;

    /** the size of the file */
    protected long size; // PART OF HEADER (8 bytes)

    /** how much bytes do we use */
    protected long filledUpTo; // PART OF HEADER (8 bytes)

    /** the access mode see <code>AccessMode</code> */
    protected AccessMode mode;

    /** the FileLock, marks if the file is used by a process */
    protected FileLock fileLock;

    /** the number of retries for opening a channel */
    protected int max_retries_connect;

    /** where the real content may start, see also <code>contentEnd</code> */
    protected long contentStart;

    /** where the real content may end, see also <code>contentStart</code> */
    protected long contentEnd;

    /** reads all relevant informations from the header */
    protected abstract void readHeader();

    /** writes all relevant informations to the header */
    protected abstract void writeHeader();

    /** enlarges the file by the local <code>incrementSize</code> but at least to the given <code>size</code>. */
    public abstract void enlargeFile(long minimumTargetSize) throws IOException;

    /**
     * writes the bytes from the given ByteBuffer to the file beginning at offset.
     * 
     * @param long offset
     * @param ByteBuffer
     *            sourceBuffer
     * @throws IOException
     */
    public abstract void write(long offset, ByteBuffer sourceBuffer) throws IOException;

    /**
     * writes the bytes from the given Byte-array to the file beginning at offset.
     * 
     * @param long offset
     * @param byte[] sourceBuffer
     * @throws IOException
     */
    public abstract void write(long offset, byte[] sourceBuffer) throws IOException;

    /**
     * appends the given sourceBuffer to the file and returns the file position of the appended entry
     * 
     * @param byte[] sourceBuffer
     * @throws IOException
     */
    public abstract long append(byte[] sourceBuffer) throws IOException;

    /**
     * appends the given sourceBuffer to the file and returns the file position of the appended entry
     * 
     * @param ByteBuffer
     *            sourceBuffer
     * @throws IOException
     */
    public abstract long append(ByteBuffer sourceBuffer) throws IOException;

    /**
     * Reads x bytes from the file to the given ByteBuffer, where x is the minimum of the capacity of the buffer and the
     * remaining written bytes in the file.
     * 
     * @param long offset
     * @param ByteBuffer
     *            destBuffer
     * @throws IOException
     */
    public abstract int read(long offset, ByteBuffer destBuffer) throws IOException;

    /**
     * Reads x bytes from the file to the given ByteBuffer, where x is the minimum of the capacity of the buffer and the
     * remaining written bytes in the file.
     * 
     * @param long offset
     * @param ByteBuffer
     *            destBuffer
     * @throws IOException
     */
    public abstract int read(long offset, byte[] destBuffer) throws IOException;

    /**
     * returns the remaining bytes between <code>filledUpTo</code> and the given <code>offset</code>
     * 
     * @param long offset
     * @return long
     */
    public long getRemainingBytes(long offset) {
        return filledUpTo - contentStart - offset;
    }

    /** sets the start of the content to zero */
    public void clear() {
        contentStart = HEADER_SIZE;
        filledUpTo = contentStart;
    }

    /** returns the offset in bytes, to where the file is filled */
    public long getFilledUpFromContentStart() {
        return filledUpTo - contentStart;
    }

    /** returns the offset in bytes, to where the file is filled */
    public long getFilledUpTo() {
        return filledUpTo;
    }

    /** returns the number of available bytes */
    public long getFreeSpace() {
        return size - filledUpTo;
    }

    /**
     * opens the {@link RandomAccessFile} and the corresponding {@link FileChannel}. Optionally reads the header.
     * 
     * @param should
     *            the header be read
     * @throws IOException
     */
    protected void openChannel(boolean readHeader) throws FileLockException, IOException {
        logger.debug("[OFile.openChannel] Opening channel for file: " + osFile);
        accessFile = new RandomAccessFile(osFile, (mode == AccessMode.READ_ONLY) ? "r" : "rw");
        accessFile.seek(0);
        channel = accessFile.getChannel();
        lock();
        if (mode == AccessMode.READ_ONLY) {
            headerBuffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, HEADER_SIZE);
        } else {
            headerBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, HEADER_SIZE);
        }
        if (readHeader) {
            readHeader();
        }
    }

    /**
     * opens the {@link RandomAccessFile} and the corresponding {@link FileChannel}. And reads the header.
     * 
     * @throws IOException
     */
    public void openChannel() throws FileLockException, IOException {
        this.openChannel(true);
    }

    /** this method will be called to init the database-file */
    protected void createFile() throws FileLockException, IOException {
        openChannel();
        filledUpTo = 0;
        accessFile.setLength(DEFAULT_SIZE);
        size = DEFAULT_SIZE;
        filledUpTo = HEADER_SIZE;
        writeHeader();
    }

    /** tries to unlock the file */
    public void unlock() throws IOException {
        System.runFinalization();
        if (fileLock != null) {
            try {
                fileLock.release();
            } catch (ClosedChannelException e) {
                throw new IOException("Can't close Channel in " + osFile + ".");
            }
            fileLock = null;
        }
    }

    /** tries to lock the file */
    public void lock() throws FileLockException, IOException {
        if (mode == AccessMode.READ_ONLY) {
            return;
        }

        for (int retries = 0; retries < max_retries_connect; retries++) {
            try {
                fileLock = channel.lock();
                break; // the lock was succesful
            } catch (OverlappingFileLockException e) {
                try {
                    logger.debug("Can't open file '" + osFile.getAbsolutePath()
                            + "' because it's locked. Waiting {} ms and retry {}", RETRY_CONNECT_WAITTIME,
                            retries);
                    System.runFinalization(); // FORCE FINALIZATION TO COLLECT ALL THE PENDING BUFFERS
                    Thread.sleep(RETRY_CONNECT_WAITTIME);
                } catch (InterruptedException ex) {
                    // will throw a FileLockException further
                }
            }
        }

        if (fileLock == null) {
            this.close();
            throw new FileLockException("File " + osFile.getPath()
                    + " is locked by another process, maybe the database is in use by another process.");
        }
    }

    /** true, if there is access to the file */
    public boolean isOpen() {
        return accessFile != null;
    }

    /**
     * checks if the accessed region is accessible
     * 
     * @param offset
     *            the byte-offset where to start
     * @param length
     *            the length of the region to access
     * @return the offset, if the region is accessible
     * @throws IOException
     */
    protected long checkRegions(long offset, int length) throws IOException {
        if (offset + length > filledUpTo)
            throw new IOException("Can't access memory outside the file size (" + filledUpTo
                    + " bytes). You've requested portion " + offset + "-" + (offset + length) + " bytes. File: "
                    + osFile.getAbsolutePath() + "\n" + "actual Filesize: " + size + "\n" + "actual filledUpTo: "
                    + filledUpTo);
        return offset;
    }

    /** closes all open channels and the file */
    public void close() {
        logger.debug("Try to close accessFile and channel for file: " + osFile);
        if (headerBuffer != null) {
            headerBuffer.force();
            headerBuffer = null;
        }

        try {
            unlock();
            if (channel != null && channel.isOpen()) {
                channel.close();
                channel = null;
            }

            if (accessFile != null) {
                accessFile.close();
                accessFile = null;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        logger.debug("Closed accessFile and channel for file: " + osFile);
    }

    /** tries to delete the file */
    public void delete() throws IOException {
        close();
        if (osFile != null) {
            boolean deleted = osFile.delete();
            while (!deleted) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    logger.error("{} not deleted.", osFile);
                }
                System.gc();
                deleted = osFile.delete();
            }
        }
    }

    /** returns the name of the underlying OS-File */
    public String getName() {
        return osFile.getName();
    }
}
