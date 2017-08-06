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
package com.unister.semweb.drums.api;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.unister.semweb.drums.file.FileLockException;
import com.unister.semweb.drums.file.HeaderIndexFile;
import com.unister.semweb.drums.storable.AbstractKVStorable;
import com.unister.semweb.drums.storable.GeneralStorable;
import com.unister.semweb.drums.util.KeyUtils;

/**
 * This class represents an efficient direct access reader. It holds all files opened for reading. Only use this Reader,
 * when there are no write-operations during reading. All files will be locked. Be careful: Opening all files may cost a
 * lot of memory, because all indices are loaded.<br>
 * <br>
 * Please use the factory-method {@link DRUMS#getReader()} to get an instance of this class.
 * 
 * @author Martin Nettling
 * @param <Data>
 *            an implementation of {@link AbstractKVStorable}, e.g. {@link GeneralStorable}
 */
public class DRUMSReader<Data extends AbstractKVStorable> {
    /** Marks if files are opened. Is set to avoid null-pointer exceptions */
    protected boolean filesAreOpened = false;

    /** An array containing all used files. All files are opened when instantiating for performance reasons. */
    private HeaderIndexFile<Data>[] files;

    /** A pointer to the used DRUMS */
    private DRUMS<Data> drums;

    /** An array which knows for each file in <code>files</code> the number of elements. (performance) */
    protected int[] cumulativeElementsPerFile;

    /** a prototype of Data */
    private Data prototype;

    private int numberOfBuckets;
    private int elementSize;

    /** temporarily used destination buffer */
    private ByteBuffer destBuffer;

    /**
     * Instantiates a new Reader for the given DRUMS. Be careful: All data files are opened and all indices are loaded
     * into memory.
     * 
     * @param drums
     * @throws FileLockException
     * @throws IOException
     */
    protected DRUMSReader(DRUMS<Data> drums) throws FileLockException, IOException {
        this.drums = drums;
        this.numberOfBuckets = drums.getHashFunction().getNumberOfBuckets();
        this.elementSize = drums.getElementSize();
        this.prototype = drums.getPrototype();
        openFiles();
    }

    /**
     * Opens all files used by the underlying HashFunction. The pointers are stored in <code>files</code>.
     * 
     * @throws FileLockException
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    public void openFiles() throws FileLockException, IOException {
        files = new HeaderIndexFile[numberOfBuckets];
        cumulativeElementsPerFile = new int[numberOfBuckets];
        int lastfile = 0;
        String path = drums.getDatabaseDirectory();
        for (int i = 0; i < numberOfBuckets; i++) {
            String filename = path + "/" + drums.getHashFunction().getFilename(i);
            if (!new File(filename).exists()) {
                cumulativeElementsPerFile[i] = 0;
            } else {
                lastfile = i;
                files[i] = new HeaderIndexFile<Data>(filename, 10, drums.gp);
                cumulativeElementsPerFile[i] = (int) (files[i].getFilledUpFromContentStart() / elementSize);
            }
            if (i > 0) {
                cumulativeElementsPerFile[i] += cumulativeElementsPerFile[i - 1];
            }
        }

        destBuffer = ByteBuffer.allocate((int) files[lastfile].getChunkSize());
        filesAreOpened = true;
    }

    /**
	 * Returns all elements between lowerKey and upperKey this function is still
	 * BUGGY.<br>
	 * <br>
	 * 
	 * @param lowerKey
	 * @param upperKey
	 * @return a list containing all elements between lowerKey and upperKey
	 * @throws IOException
	 */
	public List<Data> getRange(byte[] lowerKey, byte[] upperKey)
			throws IOException {
		if (!filesAreOpened) {
			throw new IOException(
					"The files are not opened yet. Use openFiles() to open all files.");
		}
		// estimate first and last bucket
		int lowerBucket = drums.getHashFunction().getBucketId(lowerKey);
		int upperBucket = drums.getHashFunction().getBucketId(upperKey);
		if (lowerBucket > upperBucket) {
			lowerBucket = 0;
			upperBucket = drums.getHashFunction().getNumberOfBuckets() - 1;
		}

		// estimate first chunk in the first file
		long lowerChunkOffset = files[lowerBucket].getIndex()
				.getStartOffsetOfChunkByKey(lowerKey);
		// estimate last chunk in the last file
		long upperChunkOffset = files[lowerBucket].getIndex()
				.getStartOffsetOfChunkByKey(upperKey);
		long filesize, startOffset, endOffset;
		byte[] tmpB = new byte[elementSize];

		ArrayList<Data> elements = new ArrayList<Data>();
		// run over all files
		OUTER: for (int i = lowerBucket; i <= upperBucket; i++) {
			HeaderIndexFile<Data> aktFile = files[i];
			filesize = aktFile.getFilledUpFromContentStart();

			// start reading at lowerChunkOffset in first file, else start
			// reading from beginning
			startOffset = i == lowerBucket ? lowerChunkOffset : 0;
			// stop reading at upperChunkOffset in last file, else read till end
			// fo file
			endOffset = i == upperBucket ? Math.max(
					upperChunkOffset + aktFile.getChunkSize(), filesize)
					: filesize;

			while (startOffset < endOffset) {
				destBuffer.clear();
				aktFile.read(startOffset, destBuffer);
				destBuffer.flip();
				while (destBuffer.remaining() >= elementSize) {
					destBuffer.get(tmpB); // get the element
					@SuppressWarnings("unchecked")
					Data record = (Data) prototype.fromByteBuffer(ByteBuffer
							.wrap(tmpB));
					if (KeyUtils.compareKey(record.getKey(), lowerKey) >= 0
							&& KeyUtils.compareKey(record.getKey(), upperKey) <= 0) {
						elements.add(record);
					} else if (KeyUtils.compareKey(record.getKey(), upperKey) > 0) {
						// we have read all relevant elements
						break OUTER;
					}
					startOffset += elementSize;
				}
			}
		}
		return elements;
	}

    /**
     * Returns the element which has exact the key or is the next smallest element after this key
     * 
     * @param key
     * @return the first element, which can be found before the given key
     * @throws IOException
     */
    public Data getPreviousElement(byte[] key) throws IOException {
        if (!filesAreOpened) {
            throw new IOException("The files are not opened yet. Use openFiles() to open all files.");
        }
        return null;
    }

    /**
     * Returns the element which has exact the key or is the next largest element after this key
     * 
     * @param key
     * @return the first element, which can be found after the given key
     * @throws IOException
     */
    public Data getNextElement(byte[] key) throws IOException {
        if (!filesAreOpened) {
            throw new IOException("The files are not opened yet. Use openFiles() to open all files.");
        }
        return null;
    }

    /**
     * Takes a list of keys and searches for that in all buckets.
     * 
     * @param keys
     * @return {@link ArrayList}
     * @throws DRUMSException
     * @throws IOException
     */
    public List<Data> get(long... keys) throws DRUMSException, IOException {
        return this.get(KeyUtils.toByteArray(keys));
    }

    /**
     * Takes a list of keys and searches for that in all buckets.
     * 
     * @param keys
     * @return {@link ArrayList}
     * @throws DRUMSException
     * @throws IOException
     */
    public List<Data> get(byte[]... keys) throws DRUMSException, IOException {
        if (!filesAreOpened) {
            throw new IOException("The files are not opened yet. Use openFiles() to open all files.");
        }
        List<Data> result = new ArrayList<Data>();
        IntObjectOpenHashMap<ArrayList<byte[]>> bucketKeyMapping = drums.getBucketKeyMapping(keys);
        for (IntObjectCursor<ArrayList<byte[]>> entry : bucketKeyMapping) {
            ArrayList<byte[]> keyList = entry.value;
            List<Data> readData = drums.searchForData(files[entry.key], keyList.toArray(new byte[keyList.size()][]));
            result.addAll(readData);
        }
        return result;
    }

    /** Closes all files */
    public void closeFiles() {
        filesAreOpened = false;
        for (HeaderIndexFile<Data> file : files) {
            if (file != null) {
                file.close();
            }
        }
    }
}
