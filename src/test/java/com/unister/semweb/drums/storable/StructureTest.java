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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;

import org.junit.Test;

import com.unister.semweb.drums.storable.GeneralStorable;
import com.unister.semweb.drums.storable.GeneralStructure;
import com.unister.semweb.drums.storable.GeneralStructure.Basic_Field_Types;

/**
 * This class tests the {@link GeneralStorable}. It doesn't test udpate or merge methods.
 * 
 * @author Martin Nettling
 *
 */
public class StructureTest {

    /**
     * Tests adding one key part and one value part of the type Integer
     * @throws IOException 
     */
    @Test
    public void testAddKeyAndValueInt() throws IOException {
        GeneralStructure s = new GeneralStructure();
        s.addKeyPart("name", Basic_Field_Types.Integer);
        s.addValuePart("name", Basic_Field_Types.Integer);
        assertEquals(s.keySize, 4);
        assertEquals(s.valueSize, 4);
        
        GeneralStorable g = new GeneralStorable(s);
        assertEquals(g.key.length, 4);
        assertEquals(g.value.length, 4);
        
    }
    /**
     * Tests "return false" when adding two keys or two values with the same name.
     * @throws IOException  
     */
    @Test
    public void testAddDoubleKeyAndDoubleValue() throws IOException {
        GeneralStructure s = new GeneralStructure();
        s.addKeyPart("name", Basic_Field_Types.Integer);
        assertFalse(s.addKeyPart("name", Basic_Field_Types.Integer));
        s.addValuePart("name", Basic_Field_Types.Integer);
        assertFalse(s.addValuePart("name", Basic_Field_Types.Integer));
    }
    
    @Test(expected=IOException.class)
    public void testExisingInstance() throws IOException {
        GeneralStructure s = new GeneralStructure();
        s.addKeyPart("name", Basic_Field_Types.Integer);
        
        @SuppressWarnings("unused")
        GeneralStorable g = new GeneralStorable(s);
        
        s.addKeyPart("name2", Basic_Field_Types.Integer);
    }
    
    /**
     * Tests 
     * @throws IOException 
     */
    @Test
    public void addAndGetDifferentDatatypes() throws IOException {
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
        assertEquals(27, cur.key.length);
        assertEquals(27, cur.value.length);
    }
}
