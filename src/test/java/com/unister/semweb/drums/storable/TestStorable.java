package com.unister.semweb.drums.storable;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.unister.semweb.drums.storable.AbstractKVStorable;

/**
 * A simple concrete implementation of {@link AbstractKVStorable} to making tests. The data have two different
 * attributes: first value and second value. The first one is a long and the second is an integer value.
 * 
 * @author n.thieme
 * 
 */
public class TestStorable extends AbstractKVStorable {
    /**
     * 
     */
    private static final long serialVersionUID = -4016417850362136045L;
    private static final int KEY_LENGTH = 8;
    private static final int VALUE_LENGTH = 12;

    /** Begin position of the first value within the data array. */
    private static final int firstValuePosition = 0;

    /** Begin position of the second value within the data array. */
    private static final int secondValuePosition = 8;

    public TestStorable() {
        this.key = new byte[KEY_LENGTH];
        this.value = new byte[VALUE_LENGTH];
    }

    public TestStorable(byte[] initialiseFrom) {
        this();
        ByteBuffer buffer = ByteBuffer.allocate(KEY_LENGTH + VALUE_LENGTH);
        buffer.put(initialiseFrom);
        buffer.flip();

        buffer.get(key);
        buffer.get(value);
    }

    public long getFirstValue() {
        byte[] extractedValue = Arrays.copyOfRange(value, firstValuePosition, 8);
        ByteBuffer converter = ByteBuffer.allocate(8);
        converter.put(extractedValue);
        converter.flip();
        return converter.getLong();
    }

    public int getSecondValue() {
        byte[] extractedValue = Arrays.copyOfRange(value, secondValuePosition, secondValuePosition + 4);
        ByteBuffer converter = ByteBuffer.allocate(4);
        converter.put(extractedValue);
        converter.flip();
        return converter.getInt();
    }

    public void setFirstValue(long toSet) {
        ByteBuffer converter = ByteBuffer.allocate(8);
        converter.putLong(toSet);
        overwrite(converter.array(), firstValuePosition);
    }

    public void setSecondValue(int toSet) {
        ByteBuffer converter = ByteBuffer.allocate(4);
        converter.putInt(toSet);
        overwrite(converter.array(), firstValuePosition);
    }

    private void overwrite(byte[] newValue, int startPosition) {
        int currentValuePosition = 0;
        for (int i = startPosition; i < newValue.length; i++) {
            value[i] = newValue[currentValuePosition];
            currentValuePosition++;
        }
    }

    @Override
    public AbstractKVStorable clone() {
        ByteBuffer buffer = ByteBuffer.allocate(KEY_LENGTH + VALUE_LENGTH);
        buffer.put(key).put(value);
        buffer.flip();

        TestStorable newStorable = new TestStorable();
        newStorable.initFromByteBuffer(buffer);
        return newStorable;
    }

    @Override
    public int getByteBufferSize() {
        return KEY_LENGTH + VALUE_LENGTH;
    }

    @Override
    public ByteBuffer toByteBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(KEY_LENGTH + VALUE_LENGTH);
        buffer.put(key).put(value);
        buffer.flip();
        return buffer;
    }

    @Override
    public void initFromByteBuffer(ByteBuffer bb) {
        bb.get(key);
        bb.get(value);
    }

    @Override
    public AbstractKVStorable fromByteBuffer(ByteBuffer bb) {
        TestStorable newStorable = new TestStorable();
        newStorable.initFromByteBuffer(bb);
        return newStorable;
    }

    @Override
    public AbstractKVStorable merge(AbstractKVStorable element) {
        return this.clone();
    }

    @Override
    public void update(AbstractKVStorable element) {
        TestStorable convertedElement = (TestStorable) element;
        this.setFirstValue(convertedElement.getFirstValue());
        this.setSecondValue(convertedElement.getSecondValue());
    }

    @Override
    public boolean equals(Object obj) {
        TestStorable converted = (TestStorable) obj;
        if (!Arrays.equals(key, converted.key)) {
            return false;
        }

        if (!Arrays.equals(value, converted.value)) {
            return false;
        }

        return true;

    }
}
