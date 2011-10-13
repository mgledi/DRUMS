package com.unister.semweb.sdrum.bucket;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import com.unister.semweb.sdrum.bucket.hashfunction.RangeHashFunction;
import com.unister.semweb.sdrum.file.AbstractHeaderFile.AccessMode;
import com.unister.semweb.sdrum.file.FileLockException;
import com.unister.semweb.sdrum.file.HeaderIndexFile;
import com.unister.semweb.sdrum.storable.AbstractKVStorable;

/**
 * This class manages the splitting of a bucket. It would be possible to implement all functionality of this class in a
 * static context. But for reasons of inheritance, the implementation is done in the classic way. The splitting should
 * be done by the following steps<br>
 * <li>determine new maxRangeValues <br> <li>adapt HashFunction or generate a new one <br> <li>generate files and move
 * elements according to the old and new HashFunction <br> <li>store the HashFunction
 * 
 * @author m.gleditzsch
 */
//TODO: remember KeyComposition in RangeHashFunction
public class BucketSplitter<Data extends AbstractKVStorable<Data>> {

    /** the directory of the database */
    String databaseDir;
    /** the file to split */
    HeaderIndexFile<Data> sourceFile;
    /** the old HashFunction */
    RangeHashFunction oldHashFunction;
    /** the new HashFunction */
    RangeHashFunction newHashFunction;

    /**
     * Instantiates a new BucketSplitter
     * 
     * @param databaseDir
     * @param hashFunction
     * @param bucketId
     * @param numberOfPartitions
     * @throws IOException
     * @throws FileLockException
     */
    public BucketSplitter(String databaseDir, RangeHashFunction hashFunction, int bucketId, int numberOfPartitions)
            throws IOException, FileLockException {
        this.oldHashFunction = hashFunction;
        this.databaseDir = databaseDir;
        // open the file (READ_ONLY)
        String fileName = hashFunction.getFilename(bucketId);
        this.sourceFile = new HeaderIndexFile<Data>(databaseDir + "/" + fileName, 100);

        // determine new thresholds
        byte[][] keysToInsert = determineNewLargestElements(numberOfPartitions);

        // adapt HashFunction
        this.newHashFunction = generateNewHashFunction(keysToInsert, bucketId);

        // move elements to files
        this.moveElements(sourceFile, newHashFunction, databaseDir);
        sourceFile.delete();
        // store hashfunction
        newHashFunction.writeToFile();
    }

    /**
     * moves elements from the source file to new smaller files. The filenames are generated automatically
     * 
     * @param source
     * @param targetHashfunction
     * @param workingDir
     * @throws IOException
     * @throws FileLockException
     */
    protected void moveElements(HeaderIndexFile<Data> source, RangeHashFunction targetHashfunction, String workingDir)
            throws IOException, FileLockException {
        ByteBuffer elem = ByteBuffer.allocate(source.getElementSize());
        HeaderIndexFile<Data> tmp = null;
        long offset = 0, key;
        int oldBucket = -1, newBucket;
        while (offset < source.getFilledUpFromContentStart()) {
            source.read(offset, elem);
            elem.rewind();
            key = elem.getLong();

            newBucket = targetHashfunction.getBucketId(key);
            if (newBucket != oldBucket) {
                if (tmp != null) {
                    tmp.close();
                }
                String fileName = workingDir + "/" + targetHashfunction.getFilename(newBucket);
                tmp = new HeaderIndexFile<Data>(fileName, AccessMode.READ_WRITE, 100, targetHashfunction.keySize,
                        source.getElementSize());
                oldBucket = newBucket;
            }

            tmp.append(elem);
            offset += elem.limit();
        }
        if (tmp != null)
            tmp.close();
    }

    /**
     * generates a new filename for a subbucket from the given oldName
     * 
     * @param subBucket
     * @param oldName
     * @return
     */
    protected String generateFileName(int subBucket, String oldName) {
        int dotPos = oldName.lastIndexOf(".");
        int slashPos = Math.max(oldName.lastIndexOf("/"), oldName.lastIndexOf("\\"));
        String prefix;
        String suffix;
        if (dotPos > slashPos) {

            prefix = oldName.substring(0, dotPos);
            suffix = oldName.substring(dotPos);
        } else {
            prefix = oldName;
            suffix = "";
        }
        return prefix + "_" + subBucket + suffix;
    }

    /**
     * Generates the new {@link RangeHashFunction}. Therefore it takes the oldfunction and inserts the new elements.
     * 
     * @param keysToInsert
     * @param bucketId
     * @return
     */
    protected RangeHashFunction generateNewHashFunction(byte[][] keysToInsert, int bucketId) {
        int numberOfPartitions = keysToInsert.length;
        int newSize = oldHashFunction.getNumberOfBuckets() - 1 + numberOfPartitions;
        byte[][] newMaxRangeValues = new byte[newSize][];
        int[] newBucketSizes = new int[newSize];
        String[] newFileNames = new String[newSize];

        int k = 0;
        for (int i = 0; i < oldHashFunction.getNumberOfBuckets(); i++) {
            if (i != bucketId) {
                newMaxRangeValues[k] = oldHashFunction.getMaxRange(i);
                newBucketSizes[k] = oldHashFunction.getBucketSize(i);
                newFileNames[k] = oldHashFunction.getFilename(i);
                k++;
            }
        }
        int elementsPerPart = determineElementsPerPart(numberOfPartitions);
        for (int i = oldHashFunction.getNumberOfBuckets() - 1; i < newSize; i++) {
            k = i - (oldHashFunction.getNumberOfBuckets() - 1);
            newMaxRangeValues[i] = keysToInsert[k];
            newBucketSizes[i] = (int) (0.01 * elementsPerPart);
            newFileNames[i] = generateFileName(k, oldHashFunction.getFilename(bucketId));
        }
        System.out.println(Arrays.toString(newFileNames));
        return new RangeHashFunction(
                newMaxRangeValues,
                newFileNames,
                newBucketSizes,
                oldHashFunction.getHashFunctionFile());
    }

    /**
     * Determines the number of elements when the partitions must have equal sizes
     * 
     * @param numberOfPartitions
     * @return int
     */
    protected int determineElementsPerPart(int numberOfPartitions) {
        return (int) Math.ceil((sourceFile.getFilledUpFromContentStart() /
                sourceFile.getElementSize() / numberOfPartitions));
    }

    /**
     * Determines the largest key for each new bucket. The values are stored in <code>newLargestKeys</code>
     * 
     * @throws IOException
     */
    protected byte[][] determineNewLargestElements(int numberOfPartitions) throws IOException {
        int elementsPerPart = determineElementsPerPart(numberOfPartitions);
        byte[][] keysToInsert = new byte[numberOfPartitions][];
        long offset;
        for (int i = 0; i < numberOfPartitions; i++) {
            if (i == numberOfPartitions - 1) {
                offset = sourceFile.getFilledUpFromContentStart() - sourceFile.getElementSize();
            } else {
                offset = ((i + 1) * elementsPerPart - 1) * sourceFile.getElementSize();
            }

            ByteBuffer keyBuffer = ByteBuffer.allocate(oldHashFunction.keySize);
            sourceFile.read(offset, keyBuffer);
            keyBuffer.position(0);

            keysToInsert[i] = keyBuffer.array();
        }
        return keysToInsert;
    }
}
