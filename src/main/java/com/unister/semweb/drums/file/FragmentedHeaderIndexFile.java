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

import com.unister.semweb.drums.GlobalParameters;
import com.unister.semweb.drums.storable.AbstractKVStorable;

/**
 * This class is not for use yet.
 * 
 * @author Martin Nettling
 * 
 */
public class FragmentedHeaderIndexFile<Data extends AbstractKVStorable> extends HeaderIndexFile<Data> {

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
    public FragmentedHeaderIndexFile(String fileName, AccessMode mode, int max_retries_connect,
            GlobalParameters<Data> gp)
            throws FileLockException, IOException {
        super(fileName, mode, max_retries_connect, gp);
    }

    @Override
    public void write(long offset, ByteBuffer sourceBuffer) throws IOException {
        super.write(offset, sourceBuffer);
    }

    @Override
    public int read(long offset, ByteBuffer destBuffer) throws IOException {
        return super.read(offset, destBuffer);
    }

    @Override
    public long getFreeSpace() {
        // TODO
        return 0;
    }

    // TODO: adapt read, only full chunks can be read and be written

    @SuppressWarnings("unused")
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
