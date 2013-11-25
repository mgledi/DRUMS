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
package com.unister.semweb.drums.file;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * All files used by DRUMS should implement this interface.
 * 
 * @author Martin Nettling
 */
interface IWritableIndexFile {

    /**
     * enlarges the file by the given size
     * 
     * @param size
     * @throws IOException
     */
    void enlargeFile(long size) throws IOException;

    /**
     * writes the bytes from the given ByteBuffer to the file beginning at offset.
     * 
     * @param offset
     * @param sourceBuffer
     * @throws IOException
     */
    void write(long offset, ByteBuffer sourceBuffer) throws IOException;

    /**
     * writes the bytes from the given Byte-array to the file beginning at offset.
     * 
     * @param offset
     * @param sourceBuffer
     * @throws IOException
     */
    void write(long offset, byte[] sourceBuffer) throws IOException;

    /**
     * appends the given sourceBuffer to the file and returns the file position of the appended entry
     * 
     * @param sourceBuffer
     * @return the position in the file after inserting the given sourceBuffer
     * @throws IOException
     */
    long append(byte[] sourceBuffer) throws IOException;

    /**
     * appends the given sourceBuffer to the file and returns the file position of the appended entry
     * 
     * @param sourceBuffer
     * @return the position in the file after inserting the given sourceBuffer
     * @throws IOException
     */
    long append(ByteBuffer sourceBuffer) throws IOException;

    /**
     * Reads x bytes from the file to the given ByteBuffer, where x is the minimum of the capacity of the buffer and the
     * remaining written bytes in the file.
     * 
     * @param offset
     * @param destBuffer
     * @return the number of bytes read
     * @throws IOException
     */
    int read(long offset, ByteBuffer destBuffer) throws IOException;

    /**
     * Reads x bytes from the file to the given byte array, where x is the minimum of the size of the given byte array
     * and the remaining written bytes in the file.
     * 
     * @param offset
     * @param destBuffer
     * @return the number of bytes read
     * @throws IOException
     */
    int read(long offset, byte[] destBuffer) throws IOException;
}
