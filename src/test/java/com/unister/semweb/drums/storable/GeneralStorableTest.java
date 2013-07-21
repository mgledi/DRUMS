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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Test;

import com.unister.semweb.drums.storable.GeneralStorable;
import com.unister.semweb.drums.storable.GeneralStructure;
import com.unister.semweb.drums.storable.GeneralStructure.Basic_Field_Types;

/**
 * This method tests the {@link GeneralStorable}. It mainly tests setter and getter methods, because those of the
 * {@link GeneralStorable} contain logical expressions.
 * 
 * @author Martin Gleditzsch
 */
public class GeneralStorableTest {

    @Test
    public void testByteBuffer() throws IOException, CloneNotSupportedException {
        GeneralStructure s = new GeneralStructure();
        s.addKeyPart("int", Basic_Field_Types.Integer);
        s.addKeyPart("long", Basic_Field_Types.Long);
        s.addValuePart("int", Basic_Field_Types.Integer);
        s.addValuePart("long", Basic_Field_Types.Long);
        GeneralStorable cur = new GeneralStorable(s);
        cur.setKey("int", 1024);
        cur.setKey("long", 123456789l);
        cur.setValue("int", 1024);
        cur.setValue("long", 123456789l);
        
        assertArrayEquals(new byte[]{0, 0, 4, 0, 0, 0, 0, 0, 7, 91, -51, 21,0, 0, 4, 0, 0, 0, 0, 0, 7, 91, -51, 21},cur.toByteBuffer().array());
        
        GeneralStorable cur2 = new GeneralStorable(s);
        cur2.initFromByteBuffer(cur.toByteBuffer());
        assertArrayEquals(new byte[]{0, 0, 4, 0, 0, 0, 0, 0, 7, 91, -51, 21,0, 0, 4, 0, 0, 0, 0, 0, 7, 91, -51, 21},cur2.toByteBuffer().array());
        
        GeneralStorable cur3 = (GeneralStorable) cur2.clone();
        assertArrayEquals(new byte[]{0, 0, 4, 0, 0, 0, 0, 0, 7, 91, -51, 21,0, 0, 4, 0, 0, 0, 0, 0, 7, 91, -51, 21},cur3.toByteBuffer().array());
        
    }
    
    @Test
    public void testGetterAndSetter() throws IOException {
        GeneralStructure s = new GeneralStructure();
        s.addKeyPart("byte", Basic_Field_Types.Byte);
        s.addKeyPart("char", Basic_Field_Types.Char);
        s.addKeyPart("double", Basic_Field_Types.Double);
        s.addKeyPart("float", Basic_Field_Types.Float);
        s.addKeyPart("int", Basic_Field_Types.Integer);
        s.addKeyPart("long", Basic_Field_Types.Long);

        s.addValuePart("byte", Basic_Field_Types.Byte);
        s.addValuePart("char", Basic_Field_Types.Char);
        s.addValuePart("double", Basic_Field_Types.Double);
        s.addValuePart("float", Basic_Field_Types.Float);
        s.addValuePart("int", Basic_Field_Types.Integer);
        s.addValuePart("long", Basic_Field_Types.Long);

        GeneralStorable cur = new GeneralStorable(s);
        assertEquals(54, cur.getByteBufferSize());
        
        // test set keys
        cur.setKey("byte", (byte) 123);
        cur.setKey("char", 'c');
        cur.setKey("double", 1.234d);
        cur.setKey("float", 5.67f);
        cur.setKey("int", 1024);
        cur.setKey("long", 123456789l);
        assertArrayEquals(cur.key, new byte[] { 123, 0, 99, 63, -13, -66, 118, -56, -76, 57, 88, 64, -75, 112,
                -92, 0, 0, 4, 0, 0, 0, 0, 0, 7, 91, -51, 21 });

        // test set values
        cur.setValue("byte", (byte) 123);
        cur.setValue("char", 'c');
        cur.setValue("double", 1.234d);
        cur.setValue("float", 5.67f);
        cur.setValue("int", 1024);
        cur.setValue("long", 123456789l);
        assertArrayEquals(cur.value, new byte[] { 123, 0, 99, 63, -13, -66, 118, -56, -76, 57, 88, 64, -75, 112,
                -92, 0, 0, 4, 0, 0, 0, 0, 0, 7, 91, -51, 21 });
        
        assertEquals((byte) 123, cur.getKeyAsByte("byte"));
        assertEquals('c', cur.getKeyAsChar("char"));
        assertEquals(1.234d, cur.getKeyAsDouble("double"), 1e8);
        assertEquals(5.67f, cur.getKeyAsFloat("float"), 1e8);
        assertEquals(1024, cur.getKeyAsInt("int"));
        assertEquals(123456789l, cur.getKeyAsLong("long"));
        
        assertEquals((byte) 123, cur.getValueAsByte("byte"));
        assertEquals('c', cur.getValueAsChar("char"));
        assertEquals(1.234d, cur.getValueAsDouble("double"), 1e8);
        assertEquals(5.67f, cur.getValueAsFloat("float"), 1e8);
        assertEquals(1024, cur.getValueAsInt("int"));
        assertEquals(123456789l, cur.getValueAsLong("long"));
    }
}
