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
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.ArrayUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.unister.semweb.drums.GlobalParameters;
import com.unister.semweb.drums.file.AbstractHeaderFile.AccessMode;
import com.unister.semweb.drums.storable.TestStorable;

/**
 * Tests the enlargement of the {@link HeaderIndexFile}.
 * 
 * @author Nils Thieme
 * 
 */
public class EnlargeFileTest {
    private static final String testFilename = "/tmp/headerIndexFile.db";

    /** Deletes the test file. */
    @Before
    public void initialise() throws Exception {
        FileUtils.deleteQuietly(new File(testFilename));
    }

    /** The test data fit into the begin file size and no enlargement takes place. */
    @Test
    public void noEnlargment() throws Exception {
        GlobalParameters<TestStorable> globalParameters = new GlobalParameters<TestStorable>(new TestStorable());
        globalParameters.INITIAL_FILE_SIZE = HeaderIndexFile.HEADER_SIZE;

        List<TestStorable> testdata = generateTestdata(10);

        List<Long> filePositions = writeData(testdata, globalParameters);

        List<TestStorable> readData = readFile(filePositions, globalParameters);

        Assert.assertTrue(testdata.equals(readData));
    }

    /**
     * The test data don't fit into the increment so a the file must be enlarged. After writing a bunch of test data
     * that are greater in size that the increment size another huge bunch will be written. This simulates a problem
     * that we have encountered when the enlargement is not done correctly.
     */
    @Test
    public void bigEnlargment() throws Exception {
        GlobalParameters<TestStorable> globalParameters = new GlobalParameters<TestStorable>(new TestStorable());
        globalParameters.INITIAL_INCREMENT_SIZE = 1000;
        globalParameters.INITIAL_FILE_SIZE = HeaderIndexFile.HEADER_SIZE;
        List<TestStorable> testdata = generateTestdata(50000);
        List<TestStorable> testdata2 = generateTestdata(20000);

        List<TestStorable> overallTestdata = new ArrayList<TestStorable>(testdata);
        overallTestdata.addAll(testdata2);

        List<Long> filePositions = writeData(testdata, globalParameters);
        List<Long> secondFilePositions = writeData(testdata2, globalParameters);

        List<Long> overallFilePositions = new ArrayList<Long>(filePositions);
        overallFilePositions.addAll(secondFilePositions);

        List<TestStorable> readData = readFile(overallFilePositions, globalParameters);
        Assert.assertTrue(overallTestdata.equals(readData));
    }

    @Test
    public void specialTest() throws Exception {
        GlobalParameters<TestStorable> globalParameters = new GlobalParameters<TestStorable>(new TestStorable());
        globalParameters.INITIAL_INCREMENT_SIZE = 50;
        globalParameters.INITIAL_FILE_SIZE = (int) (HeaderIndexFile.HEADER_SIZE + HeaderIndexFile.MAX_INDEX_SIZE_IN_BYTES);
        List<TestStorable> testdata = generateTestdata(3);
        List<TestStorable> testdata2 = generateTestdata(200);

        List<TestStorable> overallTestdata = new ArrayList<TestStorable>(testdata);
        overallTestdata.addAll(testdata2);

        List<Long> filePositions = writeData(testdata, globalParameters);
        List<Long> secondFilePositions = writeData(testdata2, globalParameters);

        List<Long> overallFilePositions = new ArrayList<Long>(filePositions);
        overallFilePositions.addAll(secondFilePositions);

        List<TestStorable> readData = readFile(overallFilePositions, globalParameters);
        Assert.assertTrue(overallTestdata.equals(readData));
    }

    /**
     * Writes the given test data to the test file. The <code>globalParameters</code> are used to control the increment
     * and the initial file size.
     */
    private List<Long> writeData(List<TestStorable> toWrite, GlobalParameters<TestStorable> globalParameters)
            throws Exception {
        HeaderIndexFile<TestStorable> file = new HeaderIndexFile<TestStorable>(testFilename, AccessMode.READ_WRITE, 1,
                globalParameters);
        List<Long> filePositions = new ArrayList<Long>();
        ByteBuffer overallBuffer = ByteBuffer.allocate(globalParameters.getPrototype().getByteBufferSize()
                * toWrite.size());
        long currentFilePosition = file.getFilledUpFromContentStart();
        for (TestStorable currentTestObject : toWrite) {
            ByteBuffer buffer = currentTestObject.toByteBuffer();
            overallBuffer.put(buffer);
            filePositions.add(currentFilePosition);
            currentFilePosition += globalParameters.getPrototype().getByteBufferSize();
        }
        overallBuffer.flip();
        file.append(overallBuffer);
        file.close();
        return filePositions;
    }

    /** Reads the data given by the <code>filePositions</code>. */
    private List<TestStorable> readFile(List<Long> filePositions, GlobalParameters<TestStorable> globalParameters)
            throws Exception {
        HeaderIndexFile<TestStorable> file = new HeaderIndexFile<TestStorable>(testFilename, AccessMode.READ_ONLY, 1,
                globalParameters);
        List<TestStorable> readData = new ArrayList<TestStorable>();
        for (long currentFilePosition : filePositions) {
            byte[] buffer = new byte[globalParameters.getPrototype().getByteBufferSize()];
            file.read(currentFilePosition, buffer);

            TestStorable oneReadTestStorable = new TestStorable(buffer);
            readData.add(oneReadTestStorable);
        }
        file.close();
        return readData;
    }

    /** Generates the given number of test data. */
    private List<TestStorable> generateTestdata(int numberOfTestdata) throws Exception {
        List<TestStorable> result = new ArrayList<TestStorable>();
        byte[] keyPrefix = new byte[4];
        for (int i = 0; i < numberOfTestdata; i++) {
            TestStorable newTestObject = new TestStorable();
            byte[] runningNumberAsArray = ByteBuffer.allocate(4).putInt(i).array();
            byte[] key = ArrayUtils.addAll(keyPrefix, runningNumberAsArray);

            newTestObject.key = key;
            newTestObject.setFirstValue(i);
            newTestObject.setSecondValue(i);
            result.add(newTestObject);
        }
        return result;
    }
}
