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
package com.unister.semweb.drums.file;

import java.io.File;
import java.nio.ByteBuffer;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.unister.semweb.drums.GlobalParameters;
import com.unister.semweb.drums.file.HeaderIndexFile;
import com.unister.semweb.drums.file.AbstractHeaderFile.AccessMode;
import com.unister.semweb.drums.storable.TestStorable;

/**
 * Tests for the <code>repairIndex()</code> method of the {@link HeaderIndexFile}.
 * 
 * @author n.thieme
 * 
 */
public class RepairIndexTest {
    private static final String filename = "/tmp/indexRestorer.db";

    private GlobalParameters<TestStorable> globalParameters;
    private int elementsPerChunk;

    /** Initialises the test by removing the test file. */
    @Before
    public void inititalise() throws Exception {
        FileUtils.deleteQuietly(new File(filename));
        globalParameters = new GlobalParameters<TestStorable>(new TestStorable());
        elementsPerChunk = (int) (globalParameters.INDEX_CHUNK_SIZE / globalParameters.elementSize);
    }

    /** Only one element is written to the test file. */
    @Test
    public void simpleTest() throws Exception {
        makeTestWith(1);
    }

    /** A full chunk is written to the test file. */
    @Test
    public void oneFullChunk() throws Exception {
        makeTestWith(elementsPerChunk);
    }

    /** Several chunks are written to the test file. */
    @Test
    public void severalChunks() throws Exception {
        makeTestWith(7 * elementsPerChunk);
    }

    /**
     * Makes the real test with the given number of elements to handle. The test consists of the following parts:
     * <ul>
     * <li>Write the test data to the file</li>
     * <li>The index must not contain any element (for each element the index will return the pseudo chunk id -1)</li>
     * <li>The file is opened again and the index will be repaired.</li>
     * <li>It is examined whether each element has the chunk id within the index.</li>
     * </ul>
     * 
     * @param numberOfElements
     * @throws Exception
     */
    private void makeTestWith(int numberOfElements) throws Exception {
        TestStorable[] testdata = generateTestdata(numberOfElements);
        ByteBuffer writeData = convert(testdata);

        HeaderIndexFile<TestStorable> file = new HeaderIndexFile<TestStorable>(filename, AccessMode.READ_WRITE, 1,
                globalParameters);
        file.write(0, writeData);
        Assert.assertNotNull(file.getIndex());
        Assert.assertTrue(allElementNotInIndex(testdata, file));
        file.close();

        HeaderIndexFile<TestStorable> fileToIndex = new HeaderIndexFile<TestStorable>(filename, AccessMode.READ_WRITE,
                1, globalParameters);
        fileToIndex.repairIndex();
        fileToIndex.close();

        HeaderIndexFile<TestStorable> fileWithIndex = new HeaderIndexFile<TestStorable>(filename,
                AccessMode.READ_WRITE, 1, globalParameters);
        Assert.assertTrue(isChunkId(testdata, fileWithIndex));
        fileWithIndex.close();
    }

    /**
     * Generates the given number of test data. The only difference of the {@link TestStorable} is their key (it is
     * increased by 1).
     */
    private TestStorable[] generateTestdata(int numberOfTestdata) {
        TestStorable[] result = new TestStorable[numberOfTestdata];
        for (int i = 0; i < numberOfTestdata; i++) {
            byte[] currentKey = transform(i + 1, globalParameters.keySize);
            TestStorable newData = new TestStorable();
            newData.key = currentKey;
            result[i] = newData;
        }
        return result;
    }

    /** Converts the given array of {@link TestStorable} to a consecutive {@link ByteBuffer}. */
    private ByteBuffer convert(TestStorable[] toConvert) {
        ByteBuffer converter = ByteBuffer.allocate(toConvert.length * globalParameters.elementSize);
        for (TestStorable oneTestStorable : toConvert) {
            converter.put(oneTestStorable.toByteBuffer());
        }
        converter.flip();
        return converter;
    }

    /**
     * Converts the given <code>value</code> to a byte array representation. The final array has
     * <code>numberOfBytes</code>.
     */
    private byte[] transform(int value, int numberOfBytes) {
        ByteBuffer buffer = ByteBuffer.allocate(numberOfBytes);
        buffer.putInt(numberOfBytes - 4, value);
        return buffer.array();
    }

    /** Examines whether all the given {@link TestStorable} are located in the <code>expectedChunkId</code>. */
    private boolean isChunkId(TestStorable[] TestStorable, HeaderIndexFile<TestStorable> file) {
        for (int i = 0; i < TestStorable.length; i++) {
            int expectedChunkId = i / elementsPerChunk;
            TestStorable oneTestStorable = TestStorable[i];
            int chunkId = file.getIndex().getChunkId(oneTestStorable.key);
            if (chunkId != expectedChunkId) {
                System.out.println("Expected chunk id: " + expectedChunkId + " <-> Read chunk id: " + chunkId);
                return false;
            }
        }
        return true;
    }

    /** Examines whether all given <code>elements</code> are not indexed. */
    private boolean allElementNotInIndex(TestStorable[] elements, HeaderIndexFile<TestStorable> file) {
        for (TestStorable oneTestStorable : elements) {
            int currentChunkId = file.getIndex().getChunkId(oneTestStorable.key);
            if (currentChunkId != -1) {
                return false;
            }
        }
        return true;
    }
}
