package com.unister.semweb.superfast.storage.storable;

import java.io.Serializable;

/**
 * Abstract implementation of interface {@link KVStorable}. Extend this class to build your own objects, which have to
 * be storable.
 * 
 * @author m.gleditzsch
 */
public abstract class AbstractKVStorable<Data extends AbstractKVStorable<Data>> implements KVStorable<Data>, Serializable {
    /**
     * 
     */
    private static final long serialVersionUID = -3973787626582319301L;
    /**
     * key of the object. This key is very important for storing the objects in ascending order. It have to be coded in
     * the first 8 bytes in the byte representation. The key is public for fast access.
     */
    public long key;

    /**
     * sets the key of this object. Be careful by overwriting the old key.
     * 
     * @param long key
     */
    public void setKey(long key) {
        this.key = key;
    }

    @Override
    public abstract Data clone() throws CloneNotSupportedException;

    @Override
    public long getKey() {
        return key;
    }

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
     * This method merges {@link AbstractKVStorable}s in the given array, which have the same key.
     * 
     * @param {@link AbstractKVStorable}[] toAdd, the {@link AbstractKVStorable}s to merge
     * @return {@link AbstractKVStorable}[], the merged {@link AbstractKVStorable}s
     */
    public static <Data extends AbstractKVStorable<Data>> Data[] merge(Data[] toAdd) {
        if (toAdd.length == 1) {
            return toAdd;
        }
        // estimate number of uniques
        int count = 1;
        for (int i = 0; i < toAdd.length - 1; i++) {
            if (toAdd[i].key != toAdd[i + 1].key) {
                count++;
            }
        }

        AbstractKVStorable<Data>[] realToAdd = new AbstractKVStorable[count];
        count = 0;
        // merge Elements in toAdd
        AbstractKVStorable<Data> first = toAdd[0];
        for (int k = 1; k < toAdd.length; k++) {
            while (k < toAdd.length && toAdd[k].key == first.key) {
                first = first.merge(toAdd[k]);
                k++;
            }
            realToAdd[count++] = first;
            if (k < toAdd.length) {
                first = toAdd[k];
            }
        }
        if (count < realToAdd.length && realToAdd[realToAdd.length - 2].key != first.key) {
            realToAdd[count++] = first;
        }
        return (Data[]) realToAdd;
    }
}
