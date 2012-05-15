package com.unister.semweb.sdrum.storable;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.unister.semweb.sdrum.storable.GeneralStructure.Basic_Field_Types;

/**
 * A {@link DummyKVStorable} for our test szenarios.
 * @author m.gleditzsch
 *
 */
public class DummyKVStorable extends GeneralStorable {
    private static final long serialVersionUID = -4331461212163985711L;

    public DummyKVStorable(GeneralStructure s) {
        super(s);
    }

    protected DummyKVStorable(int keyLength, int valueLength, GeneralStructure s) {
        super(keyLength, valueLength, s);
    }

    @Override
    public DummyKVStorable fromByteBuffer(ByteBuffer bb) {
        DummyKVStorable object = new DummyKVStorable(this.key.length, this.value.length, super.structure);
        object.initFromByteBuffer(bb);
        return object;
    }
    
    public static DummyKVStorable getInstance() {
        try {
            GeneralStructure s = new GeneralStructure();
            s.addKeyPart("part1", Basic_Field_Types.Long);
            s.addValuePart("parentCount", Basic_Field_Types.Integer);
            s.addValuePart("relevanceScore", Basic_Field_Types.Double);
            return new DummyKVStorable(s);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }
}
