/* Copyright (C) 2012-2013 Unister GmbH
 * 
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA. */
package com.unister.semweb.drums.storable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.unister.semweb.drums.util.Bytes;

/**
 * This class is - as the name suggests - a general storable for DRUMS. To use instances of this class you first have to
 * build the structure of a {@link GeneralStorable}. You can do this by using the methods <code>addValuePart</code> and
 * <code>addKeyPart</part>. <br><br>
 * 
 * The <code>update</code> and <code>merge</code> method are integrated very aggressive and overwrite all values an
 * instance of this class. You will have to overwrite them. <br>
 * <br>
 * Remember: If you want to be as effective as possible, you have to implement your own storable extending the
 * {@link AbstractKVStorable}.
 * 
 * @author Martin Nettling
 */
public class GeneralStorable extends AbstractKVStorable {
    static Logger logger = LoggerFactory.getLogger(GeneralStorable.class);
    private static final long serialVersionUID = 3853444781559739538L;

    /** A pointer to the underlying structure. All cloned elements should point to the same structure. */
    protected GeneralStructure structure;

    /**
     * Basic constructor. Should only be used, when the structure of the
     * 
     * @param s
     *            the structure of this {@link GeneralStorable}.
     */
    public GeneralStorable(GeneralStructure s) {
        structure = s;
        if (structure.keySize == 0) {
            logger.error("The size of the key is 0");
        }

        if (structure.valueSize == 0) {
            logger.warn("The size of the value is 0");
        }

        this.key = new byte[structure.keySize];
        this.value = new byte[structure.valueSize];
        structure.INSTANCE_EXISITS = true;
    }

    /** Constructor for cloning */
    protected GeneralStorable(int keySize, int valueSize, GeneralStructure s) {
        this.structure = s;
        this.key = new byte[keySize];
        this.value = new byte[valueSize];
        structure.INSTANCE_EXISITS = true;
    }

    @Override
    public void initFromByteBuffer(ByteBuffer bb) {
        if (bb.remaining() < key.length + value.length) {
            bb.rewind();
        }
        bb.get(key).get(value);
    }

    @Override
    public GeneralStorable fromByteBuffer(ByteBuffer bb) {
        GeneralStorable object = new GeneralStorable(this.key.length, this.value.length, structure);
        object.initFromByteBuffer(bb);
        return object;
    }

    @Override
    public ByteBuffer toByteBuffer() {
        return ByteBuffer.allocate(key.length + value.length).put(key).put(value);
    }

    @Override
    public GeneralStorable clone() {
        return this.fromByteBuffer(toByteBuffer());
    }

    @Override
    public <Data extends AbstractKVStorable> Data merge(Data element) {
        return element;
    }

    @Override
    public void update(AbstractKVStorable element) {
        element.initFromByteBuffer(this.toByteBuffer());
    }

    /**
     * Sets the value belonging to the given field.
     * 
     * @param field
     *            the name of the field
     * @param value
     *            the value to set
     * @throws IOException
     */
    public void setValue(String field, byte[] value) throws IOException {
        int hash = Arrays.hashCode(field.getBytes());
        if (!structure.valueHash2Index.containsKey(hash)) {
            throw new IOException("The field " + field + " is unknown.");
        }
        setValue(structure.valueHash2Index.get(hash), value);
    }

    /**
     * Sets the value belonging to the given field.
     * 
     * @param index
     *            the index of the requested field
     * @param value
     *            the value to set
     * @throws IOException
     */
    public void setValue(int index, byte[] value) throws IOException {
        if (index >= structure.valueSizes.size()) {
            throw new IOException("Index " + index + " is out of range.");
        }
        int length = structure.valueSizes.get(index);
        if (value.length != length) {
            throw new IOException("The length of the given value is not equal to the expected one. (" + value.length
                    + "!=" + length + ")");
        }
        Bytes.putBytes(this.value, structure.valueByteOffsets.get(index), value, 0, value.length);
    }

    /**
     * Sets the value belonging to the given field.
     * 
     * @param field
     *            the name of the field
     * @param value
     * @throws IOException
     */
    public void setValue(String field, int value) throws IOException {
        int hash = Arrays.hashCode(field.getBytes());
        setValue(structure.valueHash2Index.get(hash), value);
    }

    /**
     * Sets the value belonging to the given field.
     * 
     * @param index
     *            the index of the requested field
     * @param value
     * @throws IOException
     */
    public void setValue(int index, int value) throws IOException {
        if (index >= structure.valueSizes.size()) {
            throw new IOException("Index " + index + " is out of range.");
        }
        Bytes.putInt(this.value, structure.valueByteOffsets.get(index), value);
    }

    /**
     * Sets the value belonging to the given field.
     * 
     * @param field
     *            the name of the field
     * @param value
     * @throws IOException
     */
    public void setValue(String field, float value) throws IOException {
        int hash = Arrays.hashCode(field.getBytes());
        setValue(structure.valueHash2Index.get(hash), value);
    }

    /**
     * Sets the value belonging to the given field.
     * 
     * @param index
     *            the index of the requested field
     * @param value
     * @throws IOException
     */
    public void setValue(int index, float value) throws IOException {
        if (index >= structure.valueSizes.size()) {
            throw new IOException("Index " + index + " is out of range.");
        }
        Bytes.putFloat(this.value, structure.valueByteOffsets.get(index), value);
    }

    /**
     * Sets the value belonging to the given field.
     * 
     * @param field
     *            the name of the field
     * @param value
     * @throws IOException
     */
    public void setValue(String field, long value) throws IOException {
        int hash = Arrays.hashCode(field.getBytes());
        setValue(structure.valueHash2Index.get(hash), value);
    }

    /**
     * Sets the value belonging to the given field.
     * 
     * @param index
     *            the index of the requested field
     * @param value
     * @throws IOException
     */
    public void setValue(int index, long value) throws IOException {
        if (index >= structure.valueSizes.size()) {
            throw new IOException("Index " + index + " is out of range.");
        }
        Bytes.putLong(this.value, structure.valueByteOffsets.get(index), value);
    }

    /**
     * Sets the value belonging to the given field.
     * 
     * @param field
     *            the name of the field
     * @param value
     * @throws IOException
     */
    public void setValue(String field, double value) throws IOException {
        int hash = Arrays.hashCode(field.getBytes());
        setValue(structure.valueHash2Index.get(hash), value);
    }

    /**
     * Sets the value belonging to the given field.
     * 
     * @param index
     *            the index of the requested field
     * @param value
     * @throws IOException
     */
    public void setValue(int index, double value) throws IOException {
        if (index >= structure.valueSizes.size()) {
            throw new IOException("Index " + index + " is out of range.");
        }
        Bytes.putDouble(this.value, structure.valueByteOffsets.get(index), value);
    }

    /**
     * Sets the value belonging to the given field.
     * 
     * @param field
     *            the name of the field
     * @param value
     * @throws IOException
     */
    public void setValue(String field, char value) throws IOException {
        int hash = Arrays.hashCode(field.getBytes());
        setValue(structure.valueHash2Index.get(hash), value);
    }

    /**
     * Sets the value belonging to the given field.
     * 
     * @param index
     *            the index of the requested field
     * @param value
     * @throws IOException
     */
    public void setValue(int index, char value) throws IOException {
        if (index >= structure.valueSizes.size()) {
            throw new IOException("Index " + index + " is out of range.");
        }
        Bytes.putChar(this.value, structure.valueByteOffsets.get(index), value);
    }

    /**
     * Sets the value belonging to the given field.
     * 
     * @param field
     *            the name of the field
     * @param value
     * @throws IOException
     */
    public void setValue(String field, byte value) throws IOException {
        int hash = Arrays.hashCode(field.getBytes());
        setValue(structure.valueHash2Index.get(hash), value);
    }

    /**
     * Sets the value belonging to the given field.
     * 
     * @param index
     *            the index of the requested field
     * @param value
     * @throws IOException
     */
    public void setValue(int index, byte value) throws IOException {
        if (index >= structure.valueSizes.size()) {
            throw new IOException("Index " + index + " is out of range.");
        }
        this.value[structure.valueByteOffsets.get(index)] = value;
    }

    /**
     * Returns the value belonging to the given field as read-only ByteBuffer
     * 
     * @param field
     *            the name of the field
     * @return the requested value
     * @throws IOException
     */
    public ByteBuffer getValue(String field) throws IOException {
        int hash = Arrays.hashCode(field.getBytes());
        return getValue(structure.valueHash2Index.get(hash));
    }

    /**
     * Returns the value belonging to the given field as read-only ByteBuffer
     * 
     * @param index
     *            the index of the requested field
     * @return the requested value
     * @throws IOException
     */
    public ByteBuffer getValue(int index) throws IOException {
        if (index >= structure.valueSizes.size()) {
            throw new IOException("Index " + index + " is out of range.");
        }
        int length = structure.valueSizes.get(index);
        return ByteBuffer.wrap(value, structure.valueByteOffsets.get(index), length).asReadOnlyBuffer();
    }

    /**
     * Returns the value belonging to the given field as Int
     * 
     * @param index
     *            the index of the requested field
     * @return the requested value
     * @throws IOException
     */
    public int getValueAsInt(int index) throws IOException {
        if (index >= structure.valueSizes.size()) {
            throw new IOException("Index " + index + " is out of range.");
        }
        return Bytes.toInt(value, structure.valueByteOffsets.get(index));
    }

    /**
     * Returns the value belonging to the given field as int
     * 
     * @param field
     *            the name of the field
     * @return the requested value
     * @throws IOException
     */
    public int getValueAsInt(String field) throws IOException {
        int hash = Arrays.hashCode(field.getBytes());
        return getValueAsInt(structure.valueHash2Index.get(hash));
    }

    /**
     * Returns the value belonging to the given field as int
     * 
     * @param field
     *            the name of the field
     * @return the requested value
     * @throws IOException
     */
    public float getValueAsFloat(String field) throws IOException {
        int hash = Arrays.hashCode(field.getBytes());
        return getValueAsFloat(structure.valueHash2Index.get(hash));
    }

    /**
     * Returns the value belonging to the given field as Int
     * 
     * @param index
     *            the index of the requested field
     * @return the requested value
     * @throws IOException
     */
    public float getValueAsFloat(int index) throws IOException {
        if (index >= structure.valueSizes.size()) {
            throw new IOException("Index " + index + " is out of range.");
        }
        return Bytes.toFloat(value, structure.valueByteOffsets.get(index));
    }

    /**
     * Returns the value belonging to the given field as long
     * 
     * @param index
     *            the index of the requested field
     * @return the requested value
     * @throws IOException
     */
    public double getValueAsDouble(int index) throws IOException {
        if (index >= structure.valueSizes.size()) {
            throw new IOException("Index " + index + " is out of range.");
        }
        return Bytes.toDouble(value, structure.valueByteOffsets.get(index));
    }

    /**
     * Returns the value belonging to the given field as long
     * 
     * @param field
     *            the name of the field
     * @return the requested value
     * @throws IOException
     */
    public double getValueAsDouble(String field) throws IOException {
        int hash = Arrays.hashCode(field.getBytes());
        return getValueAsDouble(structure.valueHash2Index.get(hash));
    }

    /**
     * Returns the value belonging to the given field as long
     * 
     * @param index
     *            the index of the requested field
     * @return the requested value
     * @throws IOException
     */
    public long getValueAsLong(int index) throws IOException {
        if (index >= structure.valueSizes.size()) {
            throw new IOException("Index " + index + " is out of range.");
        }
        return Bytes.toLong(value, structure.valueByteOffsets.get(index));
    }

    /**
     * Returns the value belonging to the given field as long
     * 
     * @param field
     *            the name of the field
     * @return the requested value
     * @throws IOException
     */
    public long getValueAsLong(String field) throws IOException {
        int hash = Arrays.hashCode(field.getBytes());
        return getValueAsLong(structure.valueHash2Index.get(hash));
    }

    /**
     * Returns the value belonging to the given field as char
     * 
     * @param index
     *            the index of the requested field
     * @return the requested value
     * @throws IOException
     */
    public char getValueAsChar(int index) throws IOException {
        if (index >= structure.valueSizes.size()) {
            throw new IOException("Index " + index + " is out of range.");
        }
        return Bytes.toChar(value, structure.valueByteOffsets.get(index));
    }

    /**
     * Returns the value belonging to the given field as char
     * 
     * @param field
     *            the name of the field
     * @return the requested value
     * @throws IOException
     */
    public char getValueAsChar(String field) throws IOException {
        int hash = Arrays.hashCode(field.getBytes());
        return getValueAsChar(structure.valueHash2Index.get(hash));
    }

    /**
     * Returns the value belonging to the given field as byte
     * 
     * @param field
     *            the name of the field
     * @return the requested value
     * @throws IOException
     */
    public byte getValueAsByte(String field) throws IOException {
        int hash = Arrays.hashCode(field.getBytes());
        return getValueAsByte(structure.valueHash2Index.get(hash));
    }

    /**
     * Returns the value belonging to the given field as byte
     * 
     * @param index
     *            the index of the requested field
     * @return the requested value
     * @throws IOException
     */
    public byte getValueAsByte(int index) throws IOException {
        if (index >= structure.valueSizes.size()) {
            throw new IOException("Index " + index + " is out of range.");
        }
        return value[index];
    }

    /**
     * Sets the key belonging to the given field.
     * 
     * @param field
     *            the name of the field
     * @param key
     *            the key to set
     * @throws IOException
     */
    public void setKey(String field, byte[] key) throws IOException {
        int hash = Arrays.hashCode(field.getBytes());
        setKey(structure.keyHash2Index.get(hash), key);
    }

    /**
     * Sets the key belonging to the given field.
     * 
     * @param index
     *            the index of the requested field
     * @param key
     *            the key to set
     * @throws IOException
     */
    public void setKey(int index, byte[] key) throws IOException {
        if (index >= structure.keySizes.size()) {
            throw new IOException("Index " + index + " is out of range.");
        }
        int length = structure.keySizes.get(index);
        if (key.length != length) {
            throw new IOException("The length of the given key is not equal to the expected one. (" + key.length
                    + "!=" + length + ")");
        }
        Bytes.putBytes(this.key, structure.keyByteOffsets.get(index), key, 0, key.length);
    }

    /**
     * Sets the key belonging to the given field.
     * 
     * @param field
     *            the name of the field
     * @param key
     * @throws IOException
     */
    public void setKey(String field, int key) throws IOException {
        int hash = Arrays.hashCode(field.getBytes());
        setKey(structure.keyHash2Index.get(hash), key);
    }

    /**
     * Sets the key belonging to the given field.
     * 
     * @param index
     *            the index of the requested field
     * @param key
     * @throws IOException
     */
    public void setKey(int index, int key) throws IOException {
        if (index >= structure.keySizes.size()) {
            throw new IOException("Index " + index + " is out of range.");
        }
        int length = structure.keySizes.get(index);
        if (4 != length) {
            throw new IOException("The length of the given key is not equal to the expected one. (" + 4 + "!="
                    + length + ")");
        }
        Bytes.putInt(this.key, structure.keyByteOffsets.get(index), key);
    }

    /**
     * Sets the key belonging to the given field.
     * 
     * @param index
     *            the index of the requested field
     * @param key
     * @throws IOException
     */
    public void setKey(int index, float key) throws IOException {
        if (index >= structure.keySizes.size()) {
            throw new IOException("Index " + index + " is out of range.");
        }
        int length = structure.keySizes.get(index);
        if (4 != length) {
            throw new IOException("The length of the given key is not equal to the expected one. (" + 4 + "!="
                    + length + ")");
        }
        Bytes.putFloat(this.key, structure.keyByteOffsets.get(index), key);
    }

    /**
     * Sets the key belonging to the given field.
     * 
     * @param field
     *            the name of the field
     * @param key
     * @throws IOException
     */
    public void setKey(String field, float key) throws IOException {
        int hash = Arrays.hashCode(field.getBytes());
        setKey(structure.keyHash2Index.get(hash), key);
    }

    /**
     * Sets the key belonging to the given field.
     * 
     * @param field
     *            the name of the field
     * @param key
     * @throws IOException
     */
    public void setKey(String field, double key) throws IOException {
        int hash = Arrays.hashCode(field.getBytes());
        setKey(structure.keyHash2Index.get(hash), key);
    }

    /**
     * Sets the key belonging to the given field.
     * 
     * @param index
     *            the index of the requested field
     * @param key
     * @throws IOException
     */
    public void setKey(int index, double key) throws IOException {
        if (index >= structure.keySizes.size()) {
            throw new IOException("Index " + index + " is out of range.");
        }
        int length = structure.keySizes.get(index);
        if (8 != length) {
            throw new IOException("The length of the given key is not equal to the expected one. (" + 8 + "!="
                    + length + ")");
        }
        Bytes.putDouble(this.key, structure.keyByteOffsets.get(index), key);
    }

    /**
     * Sets the key belonging to the given field.
     * 
     * @param field
     *            the name of the field
     * @param key
     * @throws IOException
     */
    public void setKey(String field, long key) throws IOException {
        int hash = Arrays.hashCode(field.getBytes());
        setKey(structure.keyHash2Index.get(hash), key);
    }

    /**
     * Sets the key belonging to the given field.
     * 
     * @param index
     *            the index of the requested field
     * @param key
     * @throws IOException
     */
    public void setKey(int index, long key) throws IOException {
        if (index >= structure.keySizes.size()) {
            throw new IOException("Index " + index + " is out of range.");
        }
        int length = structure.keySizes.get(index);
        if (8 != length) {
            throw new IOException("The length of the given key is not equal to the expected one. (" + 8 + "!="
                    + length + ")");
        }
        Bytes.putLong(this.key, structure.keyByteOffsets.get(index), key);
    }

    /**
     * Sets the key belonging to the given field.
     * 
     * @param field
     *            the name of the field
     * @param key
     * @throws IOException
     */
    public void setKey(String field, char key) throws IOException {
        int hash = Arrays.hashCode(field.getBytes());
        setKey(structure.keyHash2Index.get(hash), key);
    }

    /**
     * Sets the key belonging to the given field.
     * 
     * @param index
     *            the index of the requested field
     * @param key
     * @throws IOException
     */
    public void setKey(int index, char key) throws IOException {
        if (index >= structure.keySizes.size()) {
            throw new IOException("Index " + index + " is out of range.");
        }
        int length = structure.keySizes.get(index);
        if (2 != length) {
            throw new IOException("The length of the given key is not equal to the expected one. (" + 2 + "!="
                    + length + ")");
        }
        Bytes.putChar(this.key, structure.keyByteOffsets.get(index), key);
    }

    /**
     * Sets the key belonging to the given field.
     * 
     * @param field
     *            the name of the field
     * @param key
     * @throws IOException
     */
    public void setKey(String field, byte key) throws IOException {
        int hash = Arrays.hashCode(field.getBytes());
        setKey(structure.keyHash2Index.get(hash), key);
    }

    /**
     * Sets the key belonging to the given field.
     * 
     * @param index
     *            the index of the requested field
     * @param key
     * @throws IOException
     */
    public void setKey(int index, byte key) throws IOException {
        if (index >= structure.keySizes.size()) {
            throw new IOException("Index " + index + " is out of range.");
        }
        int length = structure.keySizes.get(index);
        if (1 != length) {
            throw new IOException("The length of the given key is not equal to the expected one. (" + 1 + "!="
                    + length + ")");
        }
        this.key[structure.keyByteOffsets.get(index)] = key;
    }

    /**
     * Returns the key belonging to the given field as read-only ByteBuffer
     * 
     * @param field
     *            the name of the field
     * @return the requested value
     * @throws IOException
     */
    public ByteBuffer getKey(String field) throws IOException {
        int hash = Arrays.hashCode(field.getBytes());
        return getKey(structure.keyHash2Index.get(hash));
    }

    /**
     * Returns the key belonging to the given field as read-only ByteBuffer
     * 
     * @param index
     *            the index of the requested field
     * @return the requested value
     * @throws IOException
     */
    public ByteBuffer getKey(int index) throws IOException {
        if (index >= structure.keySizes.size()) {
            throw new IOException("Index " + index + " is out of range.");
        }
        int length = structure.keySizes.get(index);
        return ByteBuffer.wrap(key, structure.keyByteOffsets.get(index), length).asReadOnlyBuffer();
    }

    /**
     * Returns the key belonging to the given field as Int
     * 
     * @param index
     *            the index of the requested field
     * @return the requested value
     * @throws IOException
     */
    public int getKeyAsInt(int index) throws IOException {
        if (index >= structure.keySizes.size()) {
            throw new IOException("Index " + index + " is out of range.");
        }
        return Bytes.toInt(key, structure.keyByteOffsets.get(index));
    }

    /**
     * Returns the key belonging to the given field as int
     * 
     * @param field
     *            the name of the field
     * @return the requested value
     * @throws IOException
     */
    public int getKeyAsInt(String field) throws IOException {
        int hash = Arrays.hashCode(field.getBytes());
        return getKeyAsInt(structure.keyHash2Index.get(hash));
    }

    /**
     * Returns the key belonging to the given field as Int
     * 
     * @param index
     *            the index of the requested field
     * @return the requested value
     * @throws IOException
     */
    public float getKeyAsFloat(int index) throws IOException {
        if (index >= structure.keySizes.size()) {
            throw new IOException("Index " + index + " is out of range.");
        }

        return Bytes.toFloat(key, structure.keyByteOffsets.get(index));
    }

    /**
     * Returns the key belonging to the given field as int
     * 
     * @param field
     *            the name of the field
     * @return the requested value
     * @throws IOException
     */
    public float getKeyAsFloat(String field) throws IOException {
        int hash = Arrays.hashCode(field.getBytes());
        return getKeyAsFloat(structure.keyHash2Index.get(hash));
    }

    /**
     * Returns the key belonging to the given field as long
     * 
     * @param index
     *            the index of the requested field
     * @return the requested value
     * @throws IOException
     */
    public long getKeyAsLong(int index) throws IOException {
        if (index >= structure.keySizes.size()) {
            throw new IOException("Index " + index + " is out of range.");
        }
        return Bytes.toLong(key, structure.keyByteOffsets.get(index));
    }

    /**
     * Returns the key belonging to the given field as long
     * 
     * @param field
     *            the name of the field
     * @return the requested value
     * @throws IOException
     */
    public long getKeyAsLong(String field) throws IOException {
        int hash = Arrays.hashCode(field.getBytes());
        return getKeyAsLong(structure.keyHash2Index.get(hash));
    }

    /**
     * Returns the key belonging to the given field as long
     * 
     * @param index
     *            the index of the requested field
     * @return the requested value
     * @throws IOException
     */
    public double getKeyAsDouble(int index) throws IOException {
        if (index >= structure.keySizes.size()) {
            throw new IOException("Index " + index + " is out of range.");
        }

        return Bytes.toDouble(key, structure.keyByteOffsets.get(index));
    }

    /**
     * Returns the key belonging to the given field as long
     * 
     * @param field
     *            the name of the field
     * @return the requested value
     * @throws IOException
     */
    public double getKeyAsDouble(String field) throws IOException {
        int hash = Arrays.hashCode(field.getBytes());
        return getKeyAsDouble(structure.keyHash2Index.get(hash));
    }

    /**
     * Returns the key belonging to the given field as char
     * 
     * @param index
     *            the index of the requested field
     * @return the requested value
     * @throws IOException
     */
    public char getKeyAsChar(int index) throws IOException {
        if (index >= structure.keySizes.size()) {
            throw new IOException("Index " + index + " is out of range.");
        }

        return Bytes.toChar(key, structure.keyByteOffsets.get(index));
    }

    /**
     * Returns the key belonging to the given field as char
     * 
     * @param field
     *            the name of the field
     * @return the requested value
     * @throws IOException
     */
    public char getKeyAsChar(String field) throws IOException {
        int hash = Arrays.hashCode(field.getBytes());
        return getKeyAsChar(structure.keyHash2Index.get(hash));
    }

    /**
     * Returns the key belonging to the given field as byte
     * 
     * @param index
     *            the index of the requested field
     * @return the requested value
     * @throws IOException
     */
    public byte getKeyAsByte(int index) throws IOException {
        if (index >= structure.keySizes.size()) {
            throw new IOException("Index " + index + " is out of range.");
        }
        return key[structure.keyByteOffsets.get(index)];
    }

    /**
     * Returns the key belonging to the given field as byte
     * 
     * @param field
     *            the name of the field
     * @return the requested value
     * @throws IOException
     */
    public byte getKeyAsByte(String field) throws IOException {
        int hash = Arrays.hashCode(field.getBytes());
        return getKeyAsByte(structure.keyHash2Index.get(hash));
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (String s : structure.keyPartNames) {
            int hash = Arrays.hashCode(s.getBytes());
            sb.append("structure.keyPart: ").append(s).append("\t");
            sb.append(structure.keySizes.get(structure.keyHash2Index.get(hash))).append("\t");
            sb.append(structure.keyByteOffsets.get(structure.keyHash2Index.get(hash))).append("\n");
        }
        for (String s : structure.valuePartNames) {
            int hash = Arrays.hashCode(s.getBytes());
            sb.append("structure.valuePart: ").append(s).append("\t");
            sb.append(structure.valueSizes.get(structure.valueHash2Index.get(hash))).append("\t");
            sb.append(structure.valueByteOffsets.get(structure.valueHash2Index.get(hash))).append("\n");
        }

        return sb.toString();
    }
}
