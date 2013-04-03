package com.unister.semweb.sdrum.file;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.unister.semweb.sdrum.GlobalParameters;
import com.unister.semweb.sdrum.file.AbstractHeaderFile.AccessMode;
import com.unister.semweb.sdrum.storable.TestStorable;

/**
 * Tests the enlargement of the file when storing a specific amount of data. For example: we want to store 4 MB data
 * into the {@link HeaderIndexFile}. For the size of the file the following must apply:
 * <ul>
 * <li>size >= contentSize + headerSize</li>
 * <li>size <= contentSize + headerSize + incrementSize</li>
 * </ul>
 * 
 * @author n.thieme
 * 
 */
public class SpecificEnlargmentTest {
    private static final int KB = 1024;
    private static final int MB = 1024 * 1024;
    private static final String filename = "/tmp/dummyTest.db";
    private static final Random randomGenerator = new Random(1);
    private GlobalParameters<TestStorable> globalParameters;

    /** Initialise the test by removing the test file and initialisation the {@link GlobalParameters}. */
    @Before
    public void initialise() throws Exception {
        FileUtils.deleteQuietly(new File(filename));
        globalParameters = new GlobalParameters<TestStorable>(new TestStorable());
        globalParameters.INITIAL_FILE_SIZE = 1 * MB;
        globalParameters.INITIAL_INCREMENT_SIZE = 1 * KB;
    }

    /** The data will be written at once. */
    @Test
    public void oneWiriting() throws Exception {
        List<TestStorable> testSet = generateStorables(4 * MB);
        ByteBuffer convertedBuffer = toByteBuffer(testSet);
        HeaderIndexFile<TestStorable> file = new HeaderIndexFile<TestStorable>(filename, AccessMode.READ_WRITE, 1,
                globalParameters);
        long headerSize = file.contentStart;
        file.write(0, convertedBuffer);
        file.close();

        long fileSize = FileUtils.sizeOf(new File(filename));
        Assert.assertTrue(4 * MB + headerSize <= fileSize);
        Assert.assertTrue(4 * MB + headerSize + globalParameters.INITIAL_INCREMENT_SIZE >= fileSize);
    }

    /** Each {@link TestStorable} is written separately. */
    @Test
    public void singleAppends() throws Exception {
        List<TestStorable> testSet = generateStorables(4 * MB);
        HeaderIndexFile<TestStorable> file = new HeaderIndexFile<TestStorable>(filename, AccessMode.READ_WRITE, 1,
                globalParameters);
        long headerSize = file.contentStart;
        for (TestStorable oneElement : testSet) {
            file.append(oneElement.toByteBuffer());
        }
        file.close();

        long fileSize = FileUtils.sizeOf(new File(filename));

        Assert.assertTrue(4 * MB + headerSize <= fileSize);
        Assert.assertTrue(4 * MB + headerSize + globalParameters.INITIAL_INCREMENT_SIZE >= fileSize);
    }

    /**
     * Generates {@link TestStorable} in such a way that at most <code>maxBytes</code> will be used by the
     * {@link TestStorable}.
     */
    private List<TestStorable> generateStorables(int maxBytes) {
        int numberOfStorables = (int) Math.ceil((double) maxBytes / globalParameters.elementSize);

        List<TestStorable> result = new ArrayList<TestStorable>();
        for (int i = 0; i < numberOfStorables; i++) {
            byte[] newKey = generatePseudoKey(globalParameters.keySize);
            TestStorable newElement = new TestStorable();
            newElement.key = newKey;
            result.add(newElement);
        }
        return result;
    }

    /** Converts the given {@link List} of {@link TestStorable} to a {@link ByteBuffer}. */
    private ByteBuffer toByteBuffer(List<TestStorable> toConvert) {
        ByteBuffer buffer = ByteBuffer.allocate(toConvert.size() * globalParameters.elementSize);
        for (TestStorable oneElement : toConvert) {
            buffer.put(oneElement.toByteBuffer());
        }
        buffer.flip();
        return buffer;
    }

    /** Generates a pseudo key with the given length. */
    private byte[] generatePseudoKey(int keyLength) {
        byte[] result = new byte[keyLength];
        randomGenerator.nextBytes(result);
        return result;
    }
}
