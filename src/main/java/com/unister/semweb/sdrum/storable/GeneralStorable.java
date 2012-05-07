package com.unister.semweb.sdrum.storable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is - as the name suggests - a general storable for SDRUM. To use instances of this class you first have to
 * build the structure of a {@link GeneralStorable}. You can do this by using the methods <code>addValuePart</code> and
 * <code>addKeyPart</part>. <br><br>
 * 
 * The <code>update</code> and <code>merge</code> method are integrated very aggressive and overwrite all values an
 * instance of this class. You will have to overwrite them. <br>
 * <br>
 * Remember: If you want to be as effective as possible, you have to implement your own storable extending the
 * {@link AbstractKVStorable}.
 * 
 * @author m.gleditzsch
 */
public class GeneralStorable extends AbstractKVStorable<GeneralStorable> {
    static Logger logger = LoggerFactory.getLogger(GeneralStorable.class);
    private static final long serialVersionUID = 3853444781559739538L;

    /** All allowed basic types. */
    public static enum Basic_Field_Types {
        Byte(1), Boolean(1), Char(2), Short(2), ShortInt(2), Integer(4), Float(4), Double(8), Long(8);

        public int size;

        Basic_Field_Types(int byteSize) {
            this.size = byteSize;
        }
    }

    private static ArrayList<String> keyPartNames = new ArrayList<String>();
    private static ArrayList<String> valuePartNames = new ArrayList<String>();

    private static HashMap<Integer, Integer> keyHash2Index = new HashMap<Integer, Integer>();
    private static HashMap<Integer, Integer> valueHash2Index = new HashMap<Integer, Integer>();

    private static ArrayList<Integer> keyIndex2Hash = new ArrayList<Integer>();
    private static ArrayList<Integer> valueIndex2Hash = new ArrayList<Integer>();

    private static ArrayList<Integer> keySizes = new ArrayList<Integer>();
    private static ArrayList<Integer> valueSizes = new ArrayList<Integer>();

    private static ArrayList<Integer> keyByteOffsets = new ArrayList<Integer>();
    private static ArrayList<Integer> valueByteOffsets = new ArrayList<Integer>();

    private static int keySize = 0;
    private static int valueSize = 0;

    /**
     * Adds a new ValuePart
     * 
     * @param name
     *            the name of the key part. With this name you can access this part
     * @param size
     *            the size of the key part in bytes
     */
    public static void addValuePart(String name, int size) {
        int hash = Arrays.hashCode(name.getBytes());
        int index = valuePartNames.size();
        if (valueHash2Index.containsKey(hash)) {
            logger.error("A valuePart with the name {} already exists", name);
            return;
        }
        valuePartNames.add(name);
        valueHash2Index.put(hash, index);
        valueIndex2Hash.add(hash);
        valueSizes.add(size);
        valueByteOffsets.add(valueSize);
        valueSize += size;
    }

    /**
     * Adds a new ValuePart. This is a wrapper method for <code>addKeyPart(String, int)</code>.
     * 
     * @param name
     *            the name of the key part. With this name you can access this part
     * @param type
     *            the type of the key part.
     */
    public static void addValuePart(String name, Basic_Field_Types type) {
        addValuePart(name, type.size);
    }

    /**
     * Adds a new KeyPart
     * 
     * @param name
     *            the name of the key part. With this name you can access this part
     * @param size
     *            the size of the key part in bytes
     */
    public static void addKeyPart(String name, int size) {
        int hash = Arrays.hashCode(name.getBytes());
        int index = keyPartNames.size();
        if (keyHash2Index.containsKey(hash)) {
            logger.error("A keyPart with the name {} already exists", name);
            return;
        }
        keyPartNames.add(name);
        keyHash2Index.put(hash, index);
        keyIndex2Hash.add(hash);
        keySizes.add(size);
        keyByteOffsets.add(keySize);
        keySize += size;
    }

    /**
     * Adds a new KeyPart. This is a wrapper method for <code>addKeyPart(String, int)</code>.
     * 
     * @param name
     *            the name of the key part. With this name you can access this part
     * @param type
     *            the type of the key part.
     */
    public static void addKeyPart(String name, Basic_Field_Types type) {
        addKeyPart(name, type.size);
    }

    /**
     * Basic constructor. Should only be used, when the structure of the
     */
    public GeneralStorable() {
        if (keySize == 0) {
            logger.error("The size of the key is 0");
        }

        if (valueSize == 0) {
            logger.warn("The size of the value is 0");
        }

        this.key = new byte[keySize];
        this.value = new byte[valueSize];
    }

    /** Constructor for cloning */
    private GeneralStorable(int keySize, int valueSize) {
        this.key = new byte[keySize];
        this.value = new byte[valueSize];
    }

    @Override
    public void initFromByteBuffer(ByteBuffer bb) {
        bb.get(key).get(value);
    }

    @Override
    public GeneralStorable fromByteBuffer(ByteBuffer bb) {
        GeneralStorable object = new GeneralStorable(this.key.length, this.value.length);
        object.initFromByteBuffer(bb);
        return object;
    }

    @Override
    public ByteBuffer toByteBuffer() {
        return ByteBuffer.allocate(keySize + valueSize).put(key).put(value);
    }

    @Override
    public GeneralStorable clone() throws CloneNotSupportedException {
        return this.fromByteBuffer(toByteBuffer());
    }

    @Override
    public int getByteBufferSize() {
        return keySize + valueSize;
    }

    @Override
    public GeneralStorable merge(GeneralStorable element) {
        return element;
    }

    @Override
    public void update(GeneralStorable element) {
        this.initFromByteBuffer(element.toByteBuffer());
    }

    /**
     * Sets the value belonging to the given field.
     * 
     * @param field
     *            the name of the field
     * @return
     * @throws IOException
     */
    public void setValue(String field, byte[] value) throws IOException {
        int hash = Arrays.hashCode(field.getBytes());
        if(!valueHash2Index.containsKey(hash)) {
            throw new IOException("The field " + field + " is unknown.");
        }
        setValue(valueHash2Index.get(hash), value);
    }

    /**
     * Sets the value belonging to the given field.
     * 
     * @param index
     *            the index of the requested field
     * @return
     * @throws IOException
     */
    public void setValue(int index, byte[] value) throws IOException {
        if(index >= valueSizes.size()) {
            throw new IOException("Index " + index + " is out of range.");
        }
        int length = valueSizes.get(index);
        if (value.length != length) {
            throw new IOException("The length of the given value is not equal to the expected one. (" + value.length
                    + "!=" + length + ")");
        }
        ByteBuffer.wrap(this.value, index, length).put(value);
    }

    /**
     * Sets the value belonging to the given field.
     * 
     * @param field
     *            the name of the field
     * @param value
     * @return
     * @throws IOException
     */
    public void setValue(String field, int value) throws IOException {
        int hash = Arrays.hashCode(field.getBytes());
        setValue(valueHash2Index.get(hash), value);
    }

    /**
     * Sets the value belonging to the given field.
     * 
     * @param index
     *            the index of the requested field
     * @param value
     * @return
     * @throws IOException
     */
    public void setValue(int index, int value) throws IOException {
        if(index >= valueSizes.size()) {
            throw new IOException("Index " + index + " is out of range.");
        }
        int length = valueSizes.get(index);
        if (4 != length) {
            throw new IOException("The length of the given value is not equal to the expected one. (" + 4 + "!="
                    + length + ")");
        }
        ByteBuffer.wrap(this.value, index, length).putInt(value);
    }

    /**
     * Sets the value belonging to the given field.
     * 
     * @param field
     *            the name of the field
     * @param value
     * @return
     * @throws IOException
     */
    public void setValue(String field, long value) throws IOException {
        int hash = Arrays.hashCode(field.getBytes());
        setValue(valueHash2Index.get(hash), value);
    }

    /**
     * Sets the value belonging to the given field.
     * 
     * @param index
     *            the index of the requested field
     * @param value
     * @return
     * @throws IOException
     */
    public void setValue(int index, long value) throws IOException {
        if(index >= valueSizes.size()) {
            throw new IOException("Index " + index + " is out of range.");
        }
        int length = valueSizes.get(index);
        if (8 != length) {
            throw new IOException("The length of the given value is not equal to the expected one. (" + 8 + "!="
                    + length + ")");
        }
        ByteBuffer.wrap(this.value, index, length).putLong(value);
    }

    /**
     * Sets the value belonging to the given field.
     * 
     * @param field
     *            the name of the field
     * @param value
     * @return
     * @throws IOException
     */
    public void setValue(String field, char value) throws IOException {
        int hash = Arrays.hashCode(field.getBytes());
        setValue(valueHash2Index.get(hash), value);
    }

    /**
     * Sets the value belonging to the given field.
     * 
     * @param index
     *            the index of the requested field
     * @param value
     * @return
     * @throws IOException
     */
    public void setValue(int index, char value) throws IOException {
        if(index >= valueSizes.size()) {
            throw new IOException("Index " + index + " is out of range.");
        }
        int length = valueSizes.get(index);
        if (2 != length) {
            throw new IOException("The length of the given value is not equal to the expected one. (" + 2 + "!="
                    + length + ")");
        }
        ByteBuffer.wrap(this.value, index, length).putChar(value);
    }

    /**
     * Sets the value belonging to the given field.
     * 
     * @param field
     *            the name of the field
     * @param value
     * @return
     * @throws IOException
     */
    public void setValue(String field, byte value) throws IOException {
        int hash = Arrays.hashCode(field.getBytes());
        setValue(valueHash2Index.get(hash), value);
    }

    /**
     * Sets the value belonging to the given field.
     * 
     * @param index
     *            the index of the requested field
     * @param value
     * @return
     * @throws IOException
     */
    public void setValue(int index, byte value) throws IOException {
        if(index >= valueSizes.size()) {
            throw new IOException("Index " + index + " is out of range.");
        }
        int length = valueSizes.get(index);
        if (1 != length) {
            throw new IOException("The length of the given value is not equal to the expected one. (" + 1 + "!="
                    + length + ")");
        }
        ByteBuffer.wrap(this.value, index, length).put(value);
    }

    /**
     * Returns the value belonging to the given field as read-only ByteBuffer
     * 
     * @param field
     *            the name of the field
     * @return
     * @throws IOException 
     */
    public ByteBuffer getValue(String field) throws IOException {
        int hash = Arrays.hashCode(field.getBytes());
        return getValue(valueHash2Index.get(hash));
    }

    /**
     * Returns the value belonging to the given field as read-only ByteBuffer
     * 
     * @param index
     *            the index of the requested field
     * @return
     * @throws IOException 
     */
    public ByteBuffer getValue(int index) throws IOException {
        if(index >= valueSizes.size()) {
            throw new IOException("Index " + index + " is out of range.");
        }
        int length = valueSizes.get(index);
        return ByteBuffer.wrap(value, index, length).asReadOnlyBuffer();
    }

    /**
     * Returns the value belonging to the given field as Int
     * 
     * @param index
     *            the index of the requested field
     * @return
     * @throws IOException
     */
    public int getValueAsInt(int index) throws IOException {
        if(index >= valueSizes.size()) {
            throw new IOException("Index " + index + " is out of range.");
        }
        int length = valueSizes.get(index);
        if (length != 4) {
            throw new IOException("The length of the requested value-part is not equal to the one of Integer. ("
                    + length
                    + "!=" + 4 + ")");
        }
        return ByteBuffer.wrap(value, index, length).getInt();
    }

    /**
     * Returns the value belonging to the given field as int
     * 
     * @param field
     *            the name of the field
     * @return
     * @throws IOException
     */
    public int getValueAsInt(String field) throws IOException {
        int hash = Arrays.hashCode(field.getBytes());
        return getValueAsInt(valueHash2Index.get(hash));
    }

    /**
     * Returns the value belonging to the given field as long
     * 
     * @param index
     *            the index of the requested field
     * @return
     * @throws IOException
     */
    public long getValueAsLong(int index) throws IOException {
        if(index >= valueSizes.size()) {
            throw new IOException("Index " + index + " is out of range.");
        }
        int length = valueSizes.get(index);
        if (length != 8) {
            throw new IOException("The length of the requested value-part is not equal to the one of Long. (" + length
                    + "!=" + 8 + ")");
        }
        return ByteBuffer.wrap(value, index, length).getLong();
    }

    /**
     * Returns the value belonging to the given field as long
     * 
     * @param field
     *            the name of the field
     * @return
     * @throws IOException
     */
    public long getValueAsLong(String field) throws IOException {
        int hash = Arrays.hashCode(field.getBytes());
        return getValueAsLong(valueHash2Index.get(hash));
    }

    /**
     * Returns the value belonging to the given field as char
     * 
     * @param index
     *            the index of the requested field
     * @return
     * @throws IOException
     */
    public char getValueAsChar(int index) throws IOException {
        if(index >= valueSizes.size()) {
            throw new IOException("Index " + index + " is out of range.");
        }
        int length = valueSizes.get(index);
        if (length != 2) {
            throw new IOException("The length of the requested value-part is not equal to the one of Character. ("
                    + length
                    + "!=" + 2 + ")");
        }
        return ByteBuffer.wrap(value, index, length).getChar();
    }

    /**
     * Returns the value belonging to the given field as char
     * 
     * @param field
     *            the name of the field
     * @return
     * @throws IOException
     */
    public char getValueAsChar(String field) throws IOException {
        int hash = Arrays.hashCode(field.getBytes());
        return getValueAsChar(valueHash2Index.get(hash));
    }

    /**
     * Returns the value belonging to the given field as byte
     * 
     * @param field
     *            the name of the field
     * @return
     * @throws IOException
     */
    public byte getValueAsByte(String field) throws IOException {
        int hash = Arrays.hashCode(field.getBytes());
        return getValueAsByte(valueHash2Index.get(hash));
    }

    /**
     * Returns the value belonging to the given field as byte
     * 
     * @param index
     *            the index of the requested field
     * @return
     * @throws IOException
     */
    public byte getValueAsByte(int index) throws IOException {
        if(index >= valueSizes.size()) {
            throw new IOException("Index " + index + " is out of range.");
        }
        int length = valueSizes.get(index);
        if (length != 1) {
            throw new IOException("The length of the requested value-part is not equal to the one of Character. ("
                    + length + "!=" + 1 + ")");
        }
        return ByteBuffer.wrap(value, index, length).get();
    }

    /**
     * Sets the key belonging to the given field.
     * 
     * @param field
     *            the name of the field
     * @return
     * @throws IOException
     */
    public void setKey(String field, byte[] key) throws IOException {
        int hash = Arrays.hashCode(field.getBytes());
        setKey(keyHash2Index.get(hash), key);
    }

    /**
     * Sets the key belonging to the given field.
     * 
     * @param index
     *            the index of the requested field
     * @return
     * @throws IOException
     */
    public void setKey(int index, byte[] key) throws IOException {
        if(index >= valueSizes.size()) {
            throw new IOException("Index " + index + " is out of range.");
        }
        int length = keySizes.get(index);
        if (key.length != length) {
            throw new IOException("The length of the given key is not equal to the expected one. (" + key.length
                    + "!=" + length + ")");
        }
        ByteBuffer.wrap(this.key, index, length).put(key);
    }

    /**
     * Sets the key belonging to the given field.
     * 
     * @param field
     *            the name of the field
     * @param key
     * @return
     * @throws IOException
     */
    public void setKey(String field, int key) throws IOException {
        int hash = Arrays.hashCode(field.getBytes());
        setKey(keyHash2Index.get(hash), key);
    }

    /**
     * Sets the key belonging to the given field.
     * 
     * @param index
     *            the index of the requested field
     * @param key
     * @return
     * @throws IOException
     */
    public void setKey(int index, int key) throws IOException {
        if(index >= valueSizes.size()) {
            throw new IOException("Index " + index + " is out of range.");
        }
        int length = keySizes.get(index);
        if (4 != length) {
            throw new IOException("The length of the given key is not equal to the expected one. (" + 4 + "!="
                    + length + ")");
        }
        ByteBuffer.wrap(this.key, index, length).putInt(key);
    }

    /**
     * Sets the key belonging to the given field.
     * 
     * @param field
     *            the name of the field
     * @param key
     * @return
     * @throws IOException
     */
    public void setKey(String field, long key) throws IOException {
        int hash = Arrays.hashCode(field.getBytes());
        setKey(keyHash2Index.get(hash), key);
    }

    /**
     * Sets the key belonging to the given field.
     * 
     * @param index
     *            the index of the requested field
     * @param key
     * @return
     * @throws IOException
     */
    public void setKey(int index, long key) throws IOException {
        if(index >= valueSizes.size()) {
            throw new IOException("Index " + index + " is out of range.");
        }
        int length = keySizes.get(index);
        if (8 != length) {
            throw new IOException("The length of the given key is not equal to the expected one. (" + 8 + "!="
                    + length + ")");
        }
        ByteBuffer.wrap(this.key, index, length).putLong(key);
    }

    /**
     * Sets the key belonging to the given field.
     * 
     * @param field
     *            the name of the field
     * @param key
     * @return
     * @throws IOException
     */
    public void setKey(String field, char key) throws IOException {
        int hash = Arrays.hashCode(field.getBytes());
        setKey(keyHash2Index.get(hash), key);
    }

    /**
     * Sets the key belonging to the given field.
     * 
     * @param index
     *            the index of the requested field
     * @param key
     * @return
     * @throws IOException
     */
    public void setKey(int index, char key) throws IOException {
        if(index >= valueSizes.size()) {
            throw new IOException("Index " + index + " is out of range.");
        }
        int length = keySizes.get(index);
        if (2 != length) {
            throw new IOException("The length of the given key is not equal to the expected one. (" + 2 + "!="
                    + length + ")");
        }
        ByteBuffer.wrap(this.key, index, length).putChar(key);
    }

    /**
     * Sets the key belonging to the given field.
     * 
     * @param field
     *            the name of the field
     * @param key
     * @return
     * @throws IOException
     */
    public void setKey(String field, byte key) throws IOException {
        int hash = Arrays.hashCode(field.getBytes());
        setKey(keyHash2Index.get(hash), key);
    }

    /**
     * Sets the key belonging to the given field.
     * 
     * @param index
     *            the index of the requested field
     * @param key
     * @return
     * @throws IOException
     */
    public void setKey(int index, byte key) throws IOException {
        if(index >= valueSizes.size()) {
            throw new IOException("Index " + index + " is out of range.");
        }
        int length = keySizes.get(index);
        if (1 != length) {
            throw new IOException("The length of the given key is not equal to the expected one. (" + 1 + "!="
                    + length + ")");
        }
        ByteBuffer.wrap(this.key, index, length).put(key);
    }

    /**
     * Returns the key belonging to the given field as read-only ByteBuffer
     * 
     * @param field
     *            the name of the field
     * @return
     * @throws IOException 
     */
    public ByteBuffer getKey(String field) throws IOException {
        int hash = Arrays.hashCode(field.getBytes());
        return getKey(keyHash2Index.get(hash));
    }

    /**
     * Returns the key belonging to the given field as read-only ByteBuffer
     * 
     * @param index
     *            the index of the requested field
     * @return
     * @throws IOException 
     */
    public ByteBuffer getKey(int index) throws IOException {
        if(index >= valueSizes.size()) {
            throw new IOException("Index " + index + " is out of range.");
        }
        int length = keySizes.get(index);
        return ByteBuffer.wrap(key, index, length).asReadOnlyBuffer();
    }

    /**
     * Returns the key belonging to the given field as Int
     * 
     * @param index
     *            the index of the requested field
     * @return
     * @throws IOException
     */
    public int getKeyAsInt(int index) throws IOException {
        if(index >= valueSizes.size()) {
            throw new IOException("Index " + index + " is out of range.");
        }
        int length = keySizes.get(index);
        if (length != 4) {
            throw new IOException("The length of the requested key-part is not equal to the one of Integer. (" + length
                    + "!=" + 4 + ")");
        }
        return ByteBuffer.wrap(key, index, length).getInt();
    }

    /**
     * Returns the key belonging to the given field as int
     * 
     * @param field
     *            the name of the field
     * @return
     * @throws IOException
     */
    public int getKeyAsInt(String field) throws IOException {
        int hash = Arrays.hashCode(field.getBytes());
        return getKeyAsInt(keyHash2Index.get(hash));
    }

    /**
     * Returns the key belonging to the given field as long
     * 
     * @param index
     *            the index of the requested field
     * @return
     * @throws IOException
     */
    public long getKeyAsLong(int index) throws IOException {
        if(index >= valueSizes.size()) {
            throw new IOException("Index " + index + " is out of range.");
        }
        int length = keySizes.get(index);
        if (length != 8) {
            throw new IOException("The length of the requested key-part is not equal to the one of Long. (" + length
                    + "!=" + 8 + ")");
        }
        return ByteBuffer.wrap(key, index, length).getLong();
    }

    /**
     * Returns the key belonging to the given field as long
     * 
     * @param field
     *            the name of the field
     * @return
     * @throws IOException
     */
    public long getKeyAsLong(String field) throws IOException {
        int hash = Arrays.hashCode(field.getBytes());
        return getKeyAsLong(keyHash2Index.get(hash));
    }

    /**
     * Returns the key belonging to the given field as char
     * 
     * @param index
     *            the index of the requested field
     * @return
     * @throws IOException
     */
    public char getKeyAsChar(int index) throws IOException {
        if(index >= valueSizes.size()) {
            throw new IOException("Index " + index + " is out of range.");
        }
        int length = keySizes.get(index);
        if (length != 2) {
            throw new IOException("The length of the requested key-part is not equal to the one of Character. ("
                    + length
                    + "!=" + 2 + ")");
        }
        return ByteBuffer.wrap(key, index, length).getChar();
    }

    /**
     * Returns the key belonging to the given field as char
     * 
     * @param field
     *            the name of the field
     * @return
     * @throws IOException
     */
    public char getKeyAsChar(String field) throws IOException {
        int hash = Arrays.hashCode(field.getBytes());
        return getKeyAsChar(keyHash2Index.get(hash));
    }
    
    /**
     * Returns the key belonging to the given field as byte
     * 
     * @param index
     *            the index of the requested field
     * @return
     * @throws IOException
     */
    public byte getKeyAsByte(int index) throws IOException {
        if(index >= valueSizes.size()) {
            throw new IOException("Index " + index + " is out of range.");
        }
        int length = keySizes.get(index);
        if (length != 1) {
            throw new IOException("The length of the requested key-part is not equal to the one of Character. ("
                    + length
                    + "!=" + 1 + ")");
        }
        return ByteBuffer.wrap(key, index, length).get();
    }

    /**
     * Returns the key belonging to the given field as byte
     * 
     * @param field
     *            the name of the field
     * @return
     * @throws IOException
     */
    public byte getKeyAsByte(String field) throws IOException {
        int hash = Arrays.hashCode(field.getBytes());
        return getKeyAsByte(keyHash2Index.get(hash));
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (String s : keyPartNames) {
            int hash = Arrays.hashCode(s.getBytes());
            sb.append("keyPart: ").append(s).append("\t");
            sb.append(keySizes.get(keyHash2Index.get(hash))).append("\t");
            sb.append(keyByteOffsets.get(keyHash2Index.get(hash))).append("\n");
        }
        for (String s : valuePartNames) {
            int hash = Arrays.hashCode(s.getBytes());
            sb.append("valuePart: ").append(s).append("\t");
            sb.append(valueSizes.get(valueHash2Index.get(hash))).append("\t");
            sb.append(valueByteOffsets.get(valueHash2Index.get(hash))).append("\n");
        }

        return sb.toString();
    }

    public static void main(String[] args) throws Exception {

        GeneralStorable.addKeyPart("first", GeneralStorable.Basic_Field_Types.Integer);
        GeneralStorable.addKeyPart("second", GeneralStorable.Basic_Field_Types.Byte);
        GeneralStorable.addValuePart("int", GeneralStorable.Basic_Field_Types.Integer);
        GeneralStorable.addValuePart("blob", 16);

        GeneralStorable obj = new GeneralStorable();
        System.out.println(Arrays.toString(obj.getValue()));
        obj.setValue("int", ByteBuffer.allocate(4).putInt(400).array());
        obj.setValue("first", ByteBuffer.allocate(4).putInt(200).array());
        System.out.println(Arrays.toString(obj.getValue()));
        System.out.println(Arrays.toString(obj.getKey()));

        System.out.println(obj.getValueAsInt("int"));
    }
}
