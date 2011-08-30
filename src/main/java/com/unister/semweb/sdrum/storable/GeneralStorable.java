package com.unister.semweb.sdrum.storable;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;

public class GeneralStorable extends AbstractKVStorable<GeneralStorable>{

    /** */
    private static final long serialVersionUID = 1985459439L;

    public static enum Allowed_Field_Types {
        Char, Integer, Double, Float, Byte, Boolean, Short
    }
    
    public static int keySize;
    public static int[] offsets;
    
    byte[] data;
 
    
    public GeneralStorable(final LinkedHashMap<String, Allowed_Field_Types> fields, int useFirstValuesForKey) {
        this.key = new byte[keySize];
    }
    
    public <T> T getValue(String field) {
        return null;
    }
    
    @Override
    public void initFromByteBuffer(ByteBuffer bb) {
        bb.get(key).get(data);
    }

    @Override
    public GeneralStorable fromByteBuffer(ByteBuffer bb) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ByteBuffer toByteBuffer() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public GeneralStorable clone() throws CloneNotSupportedException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int getByteBufferSize() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public GeneralStorable merge(GeneralStorable element) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void update(GeneralStorable element) {
        // TODO Auto-generated method stub
        
    }

}
