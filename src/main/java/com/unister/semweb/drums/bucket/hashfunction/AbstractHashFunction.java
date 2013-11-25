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
package com.unister.semweb.drums.bucket.hashfunction;

import com.unister.semweb.drums.storable.AbstractKVStorable;

/**
 * This abstract class reflects the usage of a consistent hash-function. To work properly, the number of buckets (
 * {@link #getNumberOfBuckets()}) and a mapping of a key to a bucket-id ( {@link #getBucketId(byte[])}) is needed.
 * 
 * @author Martin Nettling
 */
public abstract class AbstractHashFunction {

    /** the number of buckets */
    protected int buckets;

    /** @return the number of buckets, addressed by this hash-function */
    public int getNumberOfBuckets() {
        return buckets;
    }

    /**
     * @param key
     *            the key to map to a bucket-id
     * @return the bucket-id belonging to the given key
     */
    public abstract int getBucketId(byte[] key);

    /**
     * @param key
     *            the element containing the key to map a bucket-id
     * @return the bucket-id belonging to the given {@link AbstractKVStorable}
     */
    public abstract int getBucketId(AbstractKVStorable key);

    /**
     * @param bucketId
     * @return the filename of the bucket for the given bucket-id.
     */
    public abstract String getFilename(int bucketId);

    /**
     * @param dbFilename
     * @return the bucket-id of the bucket belonging to the given filename.
     */
    public abstract int getBucketId(String dbFilename);
}
