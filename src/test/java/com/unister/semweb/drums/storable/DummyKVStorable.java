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
 * @author Martin Gleditzsch
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
