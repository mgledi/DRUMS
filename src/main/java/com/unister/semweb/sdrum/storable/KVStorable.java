package com.unister.semweb.sdrum.storable;

import java.nio.ByteBuffer;

/**
 * Interface for objects, which are storable in our KVStore. Please extend {@link AbstractKVStorable} for use.
 * 
 * @author m.gleditzsch, n.thieme
 */
public interface KVStorable<Data extends AbstractKVStorable<Data>> extends Cloneable {

    /** returns the key of the storable element */
    public long getKey();

    /** returns the size of the ByteBuffer which might represent this object */
    int getByteBufferSize();

    /** loads the object from the bytestream */
    public void initFromByteBuffer(ByteBuffer bb);

    /** loads the object from the bytestream */
    public Data fromByteBuffer(ByteBuffer bb);

    /** writes the object into a bytestream */
    public ByteBuffer toByteBuffer();
}
