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
package com.unister.semweb.drums.storable;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;

import com.unister.semweb.drums.utils.KeyUtils;

/**
 * Abstract implementation of interface {@link KVStorable}. Extend this class to build your own objects, which have to
 * be storable.
 * 
 * @author Martin Nettling
 */
public abstract class AbstractKVStorable implements Serializable, Cloneable {
    private static final long serialVersionUID = -3973787626582319301L;

    /**
     * key of the object. This key is very important for storing the objects in ascending order. It have to be coded in
     * the first bytes in the byte representation. The key is public for fast access.
     */
    public byte[] key;

    /**
     * value of the object. Storing all values in one byte-array reduces the memory amount of a materialized object
     */
    public byte[] value;

    /**
     * Sets the value of this object. The given array is not copied, only a pointer will be set
     * 
     * @param value
     */
    public void setValue(byte[] value) {
        this.value = value;
    }

    /**
     * @return a pointer to the underlying array, which contains all value-parts
     */
    public byte[] getValue() {
        return value;
    }

    /**
     * Sets the key of this object. Be careful by overwriting the old key. The given array is not copied, only a pointer
     * will be set.
     * 
     * @param key
     */
    public void setKey(byte[] key) {
        this.key = key;
    }

    /**
     * @return a pointer to the underlying array, which contains all key-parts
     */
    public byte[] getKey() {
        return key;
    }

    @Override
    public abstract AbstractKVStorable clone();

    /**
     * Returns the number of bytes needed to store key and value.
     * 
     * @return key.length + value.length
     */
    public int getSize() {
        return key.length + value.length;
    }

    /**
     * Converts the object to a {@link ByteBuffer}
     * 
     * @return
     */
    public abstract ByteBuffer toByteBuffer();

    public abstract void initFromByteBuffer(ByteBuffer bb);

    public abstract <Data extends AbstractKVStorable> Data fromByteBuffer(ByteBuffer bb);

    /**
     * merges the given {@link AbstractKVStorable} with this one by your implementation and returns an
     * {@link AbstractKVStorable}.
     * 
     * @param element
     * @return
     */
    public abstract <Data extends AbstractKVStorable> Data merge(Data element);

    /**
     * updates the given {@link AbstractKVStorable} with values from this one by your implementation
     * 
     * @param element
     * @return
     */
    public abstract <Data extends AbstractKVStorable> void update(Data element);

    /**
     * This method returns true if this element is marked as deleted.
     * 
     * @return boolean
     */
    public boolean isMarkedAsDeleted() {
        return false;
    }

    @Override
    public boolean equals(Object obj) {
        if (getClass() != obj.getClass())
            return false;
        AbstractKVStorable other = (AbstractKVStorable) obj;
        if (!Arrays.equals(key, other.key))
            return false;
        if (!Arrays.equals(value, other.value))
            return false;
        return true;
    }

    /**
     * This method merges {@link AbstractKVStorable}s in the given array, which have the same key.
     * 
     * @param {@link AbstractKVStorable}[] toAdd, the {@link AbstractKVStorable}s to merge. Expected sorted.
     * @return {@link AbstractKVStorable}[], the merged {@link AbstractKVStorable}s. Returned sorted.
     */
    @SuppressWarnings("unchecked")
    public static <Data extends AbstractKVStorable> Data[] merge(Data[] toAdd) {
        if (toAdd.length == 1) {
            return toAdd;
        }
        // estimate number of uniques
        int count = 1;
        for (int i = 0; i < toAdd.length - 1; i++) {
            if (KeyUtils.compareKey(toAdd[i].key, toAdd[i + 1].key) != 0) {
                count++;
            }
        }

        AbstractKVStorable[] realToAdd = new AbstractKVStorable[count];
        count = 0;
        // merge Elements in toAdd
        AbstractKVStorable first = toAdd[0];
        for (int k = 1; k < toAdd.length; k++) {
            while (k < toAdd.length && KeyUtils.compareKey(toAdd[k].key, first.key) == 0) {
                first = first.merge(toAdd[k]);
                k++;
            }
            realToAdd[count++] = first;
            if (k < toAdd.length) {
                first = toAdd[k];
            }
        }
        if (count < realToAdd.length && KeyUtils.compareKey(realToAdd[realToAdd.length - 2].key, first.key) != 0) {
            realToAdd[count++] = first;
        }
        return (Data[]) realToAdd;
    }
}
