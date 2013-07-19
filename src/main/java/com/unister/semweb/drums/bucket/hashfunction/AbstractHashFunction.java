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
package com.unister.semweb.drums.bucket.hashfunction;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.unister.semweb.drums.file.FileLockException;
import com.unister.semweb.drums.file.HeaderIndexFile;
import com.unister.semweb.drums.storable.AbstractKVStorable;

/**
 * This abstract class reflects the usage of the hash-function. If the concrete implementation is correct, we just need
 * to know the possible range of the hashFunction (<code>getNumberOfBuckets</code>) and a transformation from a
 * key-value to a bucket-id (<code>getBucketId()</code>).
 * 
 * @author m.gleditzsch
 */
public abstract class AbstractHashFunction {

    public int keySize;

    /** the number of buckets */
    protected int buckets;

    /** returns the number of buckets, adressed by this hashfunction */
    public int getNumberOfBuckets() {
        return buckets;
    }

    /** returns the bucket-id belonging to the given key */
    public abstract int getBucketId(byte[] key);

    public int getBucketId(long key) {
        byte[] b = new byte[8];
        ByteBuffer.wrap(b).putLong(key);
        return getBucketId(b);
    }

    /** returns the bucket-id belonging to the given {@link KVStorable} */
    public abstract int getBucketId(AbstractKVStorable key);

    /** Returns the filename of the bucket with the given bucket. */
    public abstract String getFilename(int bucketId);

    /** Returns the bucketid of the bucket belonging to the given filenam. */
    public abstract int getBucketId(String dbFilename);

    /**
     * This function estimates the optimal bucket size for the given bucket-id from the already stored file. The given
     * threshold shows how many bytes relative to the already store bytes should be hold in memory.
     * 
     * @param threshold
     *            a value between 0 and 1
     * @param bucketId
     *            the id of the bucket
     * @throws IOException
     * @throws FileLockException
     * @return int the size of the bucket
     */
    public static <Data extends AbstractKVStorable> int estimateOptimalBucketSize(AbstractHashFunction hashfunction,
            double threshold, int bucketId)
            throws FileLockException, IOException {
        if (threshold > 1) {
            threshold = 1;
        } else if (threshold < 0) {
            threshold = 0.01;
        }

        String fileName = hashfunction.getFilename(bucketId);
        HeaderIndexFile<Data> file = new HeaderIndexFile<Data>(fileName, 100, null);
        return (int) (file.getFilledUpFromContentStart() / file.getElementSize());
    }

    /**
     * This function estimates the optimal bucket sizes for all buckets from the already stored file. The given
     * threshold shows how many bytes relative to the already store bytes should be hold in memory.
     * 
     * @param threshold
     *            a value between 0 and 1
     * @throws IOException
     * @throws FileLockException
     * @return int[] the sizes of the buckets
     */
    public static <Data extends AbstractKVStorable> int[] estimateOptimalBucketSizes(AbstractHashFunction hashfunction,
            double threshold)
            throws FileLockException, IOException {
        int[] sizes = new int[hashfunction.getNumberOfBuckets()];
        for (int i = 0; i < hashfunction.getNumberOfBuckets(); i++) {
            String fileName = hashfunction.getFilename(i);
            HeaderIndexFile<Data> file = new HeaderIndexFile<Data>(fileName, 100, null);
            sizes[i] = (int) (file.getFilledUpFromContentStart() / file.getElementSize());
        }
        return sizes;
    }
}
