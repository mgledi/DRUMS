package com.unister.semweb.sdrum.storable;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Test;

import com.unister.semweb.sdrum.storable.GeneralStructure.Basic_Field_Types;

/**
 * This method tests the {@link GeneralStorable}. It mainly tests setter and getter methods, because those of the
 * {@link GeneralStorable} contain logical expressions.
 * 
 * @author m.gleditzsch
 */
public class GeneralStorableTest {

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
