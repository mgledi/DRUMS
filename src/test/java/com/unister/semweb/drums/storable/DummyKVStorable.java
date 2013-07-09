package com.unister.semweb.drums.storable;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.unister.semweb.drums.storable.AbstractKVStorable;
import com.unister.semweb.drums.storable.GeneralStorable;
import com.unister.semweb.drums.storable.GeneralStructure;
import com.unister.semweb.drums.storable.GeneralStructure.Basic_Field_Types;

/**
 * A {@link DummyKVStorable} for our test szenarios.
 * 
 * @author m.gleditzsch
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
    public AbstractKVStorable merge(AbstractKVStorable element) {
        DummyKVStorable date = (DummyKVStorable) element;
        try {
            date.setValue("parentCount", date.getValueAsInt("parentCount") + this.getValueAsInt("parentCount"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return date;
    }

    @Override
    public DummyKVStorable fromByteBuffer(ByteBuffer bb) {
        DummyKVStorable object = new DummyKVStorable(this.key.length, this.value.length, super.structure);
        object.initFromByteBuffer(bb);
        return object;
    }

    @Override
    public String toString() {
        try {
            return "Key: " + getKeyAsLong("key") + " | ParentCount: " + getValueAsInt("parentCount") + " | Relevance: "
                    + getValueAsDouble("relevanceScore");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "";
    }

    public static DummyKVStorable getInstance() {
        try {
            GeneralStructure s = new GeneralStructure();
            s.addKeyPart("key", Basic_Field_Types.Long);
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
