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

import com.unister.semweb.drums.util.KeyUtils;

/**
 * Abstract implementation of interface {@link AbstractKVStorable}. Extend this class to build your own storable
 * objects.
 * 
 * @author Martin Nettling
 */
public abstract class AbstractKVStorable implements Serializable, Cloneable {
    private static final long serialVersionUID = -3973787626582319301L;

    /**
     * key of the object. This key is very important for storing the objects in ascending order. It have to be coded in
     * the first bytes in the byte representation. The key is public for fast access.
     */
    protected byte[] key;

    /**
     * value of the object. Storing all values in one byte-array reduces the memory amount of a materialized object
     */
    protected byte[] value;

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
     * @return the object as {@link ByteBuffer}
     */
    public abstract ByteBuffer toByteBuffer();

    /**
     * Initializes the object from the given {@link ByteBuffer}
     * 
     * @param bb
     */
    public abstract void initFromByteBuffer(ByteBuffer bb);

    /**
     * Generates a new Object from the given {@link ByteBuffer}.
     * 
     * @param bb
     * @return a new instances of this Class
     */
    public abstract <Data extends AbstractKVStorable> Data fromByteBuffer(ByteBuffer bb);

    /**
     * Merges the given {@link AbstractKVStorable} with this one by your implementation.
     * 
     * @param element
     * @return a pointer to an {@link AbstractKVStorable}.
     */
    public abstract <Data extends AbstractKVStorable> Data merge(Data element);

    /**
     * Updates the given {@link AbstractKVStorable} with values from this one.
     * 
     * @param element
     */
    public abstract <Data extends AbstractKVStorable> void update(Data element);

    /**
     * This method returns false by default. This method must be overwritten in the concrete class,
     * 
     * @return true, if this element is marked as deleted
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
     * This method merges all {@link AbstractKVStorable}s in the given array with same keys. The array must been sorted.
     * 
     * @param toAdd
     *            this array might contain duplicate entries concerning the key, which must be merged.
     * 
     * @return a new array containing unique {@link AbstractKVStorable}s. The array is sorted.
     */
    @SuppressWarnings("unchecked")
    public static <Data extends AbstractKVStorable> Data[] merge(Data[] toAdd) {
        if (toAdd.length == 1) {
            return toAdd;
        }
        // estimate the number of unique elements and check the precondition, that the array must been sorted
        int count = 1;
        for (int i = 0; i < toAdd.length - 1; i++) {
            if (KeyUtils.compareKey(toAdd[i].key, toAdd[i + 1].key) <= 0) {
                count++;
            } else {
                throw new RuntimeException("The given array is not sorted.");
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
