package com.unister.semweb.sdrum.bucket;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.carrotsearch.hppc.IntArrayList;
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
// TODO: remember KeyComposition in RangeHashFunction
public class BucketSplitter<Data extends AbstractKVStorable> {

    /** the directory of the database */
    protected String databaseDir;

    /** the file to split */
    protected HeaderIndexFile sourceFile;

    /** the old HashFunction */
    protected RangeHashFunction hashFunction;

    /** the new HashFunction */
    // protected RangeHashFunction newHashFunction;

    /** An arraylist containing the new bucketIds */
    protected IntArrayList newBucketIds;

    /** the old bucket-id */
    protected int oldBucketId;

    protected int keySize;
    protected Data prototype;

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
    public BucketSplitter(String databaseDir, RangeHashFunction hashFunction, Data prototype) {
        this.hashFunction = hashFunction;
        this.keySize = hashFunction.keySize;
        // this.newHashFunction = hashFunction.copy();
        this.databaseDir = databaseDir;
        this.prototype = prototype;
    }

    public void splitAndStoreConfiguration(int bucketId, int numberOfPartitions) throws IOException, FileLockException {
        this.oldBucketId = bucketId;
        // open the file (READ_ONLY)
        String fileName = hashFunction.getFilename(bucketId);
        this.sourceFile = new HeaderIndexFile(databaseDir + "/" + fileName, AccessMode.READ_WRITE, 100,
                hashFunction.keySize,
                prototype.getByteBufferSize());

        // determine new thresholds
        byte[][] keysToInsert = determineNewLargestElements(numberOfPartitions);

        // We replace the last threshold with the original value. So the theoretical border of the bucket remains.
        byte[] lastKey = hashFunction.getMaxRange(bucketId);
        keysToInsert[keysToInsert.length - 1] = lastKey;

        // Replace the old bucket line with the new ones.
        hashFunction.replace(bucketId, keysToInsert);

        // move elements to files
        this.moveElements(sourceFile, hashFunction, databaseDir);
        sourceFile.delete();
        
        // store hashfunction
        hashFunction.writeToFile();

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
    protected void moveElements(HeaderIndexFile source, RangeHashFunction targetHashfunction, String workingDir)
            throws IOException, FileLockException {
        ByteBuffer elem = ByteBuffer.allocate(source.getElementSize());
        HeaderIndexFile tmp = null;
        newBucketIds = new IntArrayList();
        long offset = 0;
        byte[] key = new byte[keySize];
        int oldBucket = -1, newBucket;
        while (offset < source.getFilledUpFromContentStart()) {
            source.read(offset, elem);
            elem.rewind();
            elem.get(key);

            newBucket = targetHashfunction.getBucketId(key);
            if (newBucket != oldBucket) {
                this.newBucketIds.add(newBucket);
                if (tmp != null) {
                    tmp.close();
                }
                String fileName = workingDir + "/" + targetHashfunction.getFilename(newBucket);
                tmp = new HeaderIndexFile(fileName, AccessMode.READ_WRITE, 100, targetHashfunction.keySize,
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
     * Determines the number of elements when the partitions must have equal sizes
     * 
     * @param numberOfPartitions
     * @return int
     */
    protected int determineElementsPerPart(int numberOfPartitions) {
        double numberOfElementsWithinFile = sourceFile.getFilledUpFromContentStart() / sourceFile.getElementSize();
        double elementsPerPart = numberOfElementsWithinFile / numberOfPartitions;
        int roundNumber = (int) Math.ceil(elementsPerPart);
        return roundNumber;
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
                // Handling of last partition
                offset = sourceFile.getFilledUpFromContentStart() - sourceFile.getElementSize();
            } else {
                // Handling of all other partitions
                offset = ((i + 1) * elementsPerPart - 1) * sourceFile.getElementSize();
            }

            ByteBuffer keyBuffer = ByteBuffer.allocate(hashFunction.keySize);
            sourceFile.read(offset, keyBuffer);
            keyBuffer.position(0);

            keysToInsert[i] = keyBuffer.array();
        }
        return keysToInsert;
    }
}
