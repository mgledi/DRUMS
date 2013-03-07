package com.unister.semweb.sdrum.file;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface IWritableIndexFile {

    /** enlarges the file by the local <code>incrementSize</code> at least to the given size. */
    public abstract void enlargeFile(long atLeastTargetSize) throws IOException;

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
}
