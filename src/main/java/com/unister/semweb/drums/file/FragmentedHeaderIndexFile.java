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

import java.io.IOException;
import java.nio.ByteBuffer;

import com.unister.semweb.drums.GlobalParameters;

/**
 * This class represents a file, which contains a bunch of datasets. The file also contains a header with some
 * informations and an {@link IndexForHeaderIndexFile}. This class is for managing and handling equal sized storable
 * elements. So be very careful, if this precondition is not fulfilled.<br>
 * <br>
 * 
 * Header structure:<br/>
 * <br/>
 * <code>
 * +-----------+--------------+---------------+---------------+-------------+<br/>
 * | FILE SIZE | FILLED UP TO | Closed Softly | ReadChunkSize | ElementSize |<br/>
 * | 8 bytes . | 8 bytes .... | 1 bytes ..... | 4 bytes ..... | 4 bytes ... |<br/>
 * +-----------+--------------+---------------+---------------+-------------+<br/>
 * </code> = 1024 bytes (to have enough space for more values)<br/>
 * <br/>
 * 
 * To use this class correctly, have a look at the following methods: <li>read(long offset, ByteBuffer destBuffer) <li>
 * write(long offset, ByteBuffer sourceBuffer) <li>append(ByteBuffer sourceBuffer) <li>dbfile.getFilledUpToExclHeader()
 * <br>
 * <br>
 * The file enlarges automatically. Deletes (writing empty byte[] to a specific position) are leading to fragmentation.
 * The actual version don't have a fragmentation-handling.
 * 
 * Example code for reading the whole file:
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
 * @author m.gleditzsch
 * 
 */
public class FragmentedHeaderIndexFile extends HeaderIndexFile {

    /**
     * This constructor instantiates a new {@link FragmentedHeaderIndexFile} with the given <code>fileName</code> in the
     * given {@link AccessMode}.
     * 
     * @param fileName
     *            the filename of the underlying OSfile.
     * @param mode
     *            the mode the file should be accessed. READ_ONLY or READ_WRITE
     * @param max_retries_connect
     *            the number of retries to open a channel, if the file is locked
     * @param gp
     * @throws FileLockException
     *             if the <code>max_retries_connect</code> is exceeded
     * @throws IOException
     *             if another error with the fileaccess occured
     */
    public FragmentedHeaderIndexFile(String fileName, AccessMode mode, int max_retries_connect, GlobalParameters gp)
            throws FileLockException, IOException {
        super(fileName, mode, max_retries_connect, gp);
    }

    @Override
    public void write(long offset, ByteBuffer sourceBuffer) throws IOException {
        // TODO nicht über chunk-grenze schreiben
        super.write(offset, sourceBuffer);
    }

    @Override
    public int read(long offset, ByteBuffer destBuffer) throws IOException {
        // TODO nicht über chunk-grenze lesen
        return super.read(offset, destBuffer);
    }

    @Override
    public long getFreeSpace() {
        // TODO
        return 0;
    }

    // TODO: adapt read, only full chunks can be read and be written

    protected void fragementate() {
        if (chunkSize % elementSize != 0) {
            throw new RuntimeException();
        }
        // set needed vars
        long offset = 0; // start offset
        long useableBytes = (this.contentEnd - contentStart); // useable bytes
        int chunks = (int) Math.floor(useableBytes / chunkSize); // number of possible chunks
        int elementsPerChunk = chunkSize / elementSize; // number of elements per chunk
        int elementsPerChunkToWrite = elementsPerChunk / 2; // number of elements per chunk after fragmenting

        // System.out.println(chunks);

        // calculate documents per chunk
        // move elements within file (see Synchronizer process)
    }
}
