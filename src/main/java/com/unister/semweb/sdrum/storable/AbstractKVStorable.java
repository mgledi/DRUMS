package com.unister.semweb.sdrum.storable;

import java.io.Serializable;

import com.unister.semweb.sdrum.utils.KeyUtils;

/**
 * Abstract implementation of interface {@link KVStorable}. Extend this class to build your own objects, which have to
 * be storable.
 * 
 * @author m.gleditzsch
 */
public abstract class AbstractKVStorable<Data extends AbstractKVStorable<Data>>
        implements
        KVStorable<Data>,
        Serializable {
    /**
     * 
     */
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

    public byte[] getValue() {
        return value;
    }

    /**
     * Sets the key of this object. Be careful by overwriting the old key. The given array is not copied, only a
     * pointer will be set.
     * 
     * @param key
     */
    public void setKey(byte[] key) {
        this.key = key;
    }

    @Override
    public byte[] getKey() {
        return key;
    }

    @Override
    public abstract Data clone() throws CloneNotSupportedException;

    @Override
    public abstract int getByteBufferSize();

    /**
     * merges the given {@link AbstractKVStorable} with this one by your implementation and returns an
     * {@link AbstractKVStorable}.
     * 
     * @param element
     * @return
     */
    public abstract Data merge(Data element);

    /**
     * updates this {@link AbstractKVStorable} with values from the given one by your implementation
     * 
     * @param element
     * @return
     */
    public abstract void update(Data element);

    /**
     * This method returns true if this element is marked as deleted.
     * 
     * @return boolean
     */
    public boolean isMarkedAsDeleted() {
        return false;
    }

    /**
     * This method merges {@link AbstractKVStorable}s in the given array, which have the same key.
     * 
     * @param {@link AbstractKVStorable}[] toAdd, the {@link AbstractKVStorable}s to merge
     * @return {@link AbstractKVStorable}[], the merged {@link AbstractKVStorable}s
     */
    @SuppressWarnings("unchecked")
    public static <Data extends AbstractKVStorable<Data>> Data[] merge(Data[] toAdd) {
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

        AbstractKVStorable<Data>[] realToAdd = new AbstractKVStorable[count];
        count = 0;
        // merge Elements in toAdd
        AbstractKVStorable<Data> first = toAdd[0];
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
