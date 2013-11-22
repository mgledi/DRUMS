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
package com.unister.semweb.drums.bucket;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import com.carrotsearch.hppc.IntArrayList;
import com.unister.semweb.drums.GlobalParameters;
import com.unister.semweb.drums.bucket.hashfunction.RangeHashFunction;
import com.unister.semweb.drums.file.FileLockException;
import com.unister.semweb.drums.file.HeaderIndexFile;
import com.unister.semweb.drums.file.AbstractHeaderFile.AccessMode;
import com.unister.semweb.drums.storable.AbstractKVStorable;

/**
 * This class manages the splitting of a bucket. It would be possible to implement all functionality of this class in a
 * static context. But for reasons of inheritance, the implementation is done this way. The splitting should be done by
 * the following steps<br>
 * 
 * <li>determine new maxRangeValues <br> <li>adapt HashFunction or generate a new one <br> <li>generate files and move
 * elements according to the old and new HashFunction <br> <li>store the HashFunction
 * 
 * @author Martin Nettling
 */
// TODO: remember KeyComposition in RangeHashFunction
public class BucketSplitter<Data extends AbstractKVStorable> {
    /** the file to split */
    protected HeaderIndexFile<Data> sourceFile;

    /** the old HashFunction */
    protected RangeHashFunction hashFunction;

    /** An arraylist containing the new bucketIds */
    protected IntArrayList newBucketIds;

    /** the old bucket-id */
    protected int oldBucketId;

    /** A pointer to the GlobalParameters of this DRUMS */
    protected GlobalParameters<Data> gp;

    /**
     * Instantiates a new BucketSplitter
     * 
     * @param databaseDir
     * @param hashFunction
     * @param gp
     * @throws IOException
     * @throws FileLockException
     */
    public BucketSplitter(RangeHashFunction hashFunction, GlobalParameters<Data> gp) {
        this.gp = gp;
        this.hashFunction = hashFunction;
        // this.newHashFunction = hashFunction.copy();
    }

    public void splitAndStoreConfiguration(int bucketId, int numberOfPartitions) throws IOException, FileLockException {
        this.oldBucketId = bucketId;
        // open the file (READ_ONLY)
        String fileName = hashFunction.getFilename(bucketId);
        this.sourceFile = new HeaderIndexFile<Data>(gp.databaseDirectory + "/" + fileName, AccessMode.READ_WRITE, 100,
                gp);

        // determine new thresholds
        byte[][] keysToInsert = determineNewLargestElements(numberOfPartitions);
        // We replace the last threshold with the original value. So the theoretical border of the bucket remains.
        byte[] lastKey = hashFunction.getMaxRange(bucketId);
        keysToInsert[keysToInsert.length - 1] = lastKey;

        // Replace the old bucket line with the new ones.
        hashFunction.replace(bucketId, keysToInsert);

        // move elements to files
        this.moveElements(sourceFile, hashFunction, gp.databaseDirectory);
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
    protected void moveElements(HeaderIndexFile<Data> source, RangeHashFunction targetHashfunction, String workingDir)
            throws IOException, FileLockException {
        ByteBuffer elem = ByteBuffer.allocate(source.getElementSize());
        HeaderIndexFile<Data> tmp = null;
        newBucketIds = new IntArrayList();
        long offset = 0;
        byte[] key = new byte[gp.keySize];
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
                tmp = new HeaderIndexFile<Data>(fileName, AccessMode.READ_WRITE, 100, gp);
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
