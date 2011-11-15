package com.unister.semweb.sdrum.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KeySearcherTest {
    private static final Logger log = LoggerFactory.getLogger(KeySearcherTest.class);
    private static Random randomGenerator;

    @BeforeClass
    public static void initialise() {
        randomGenerator = new Random(1);
    }

    @Test
    public void searchMachineTest() {
        byte[][] generatedData = generateKeysSorted(1000);
        List<byte[]> keysToSearchFor = takeKeys(generatedData, 1000);

        for (byte[] oneKey : keysToSearchFor) {
            log.info("Searching for key {}", KeyUtils.transform(oneKey));
            int foundPosition = KeySearcher.searchFor(generatedData, oneKey);

            int indexWithinOriginal = getIndexOf(keysToSearchFor, oneKey);

            Assert.assertEquals(indexWithinOriginal, foundPosition);
        }
    }

    @Test
    public void noResultFound() {
        byte[][] generatedData = generateKeysSorted(100);
        byte[] toSearchFor = new byte[] { (byte) 255, (byte) 255, (byte) 255, (byte) 245, (byte) 231, (byte) 234,
                (byte) 246, (byte) 267 };

        int foundPosition = KeySearcher.searchFor(generatedData, toSearchFor);
        Assert.assertEquals(-1, foundPosition);
    }

    private int getIndexOf(List<byte[]> list, byte[] searchFor) {
        for (int i = 0; i < list.size(); i++) {
            if (KeyUtils.compareKey(list.get(i), searchFor) == 0) {
                return i;
            }
        }
        return -1;
    }

    private byte[][] generateKeysSorted(int numberOfKeys) {
        byte[][] generated = new byte[numberOfKeys][8];
        for (int i = 0; i < numberOfKeys; i++) {
            byte[] oneKey = KeyUtils.transformFromLong(i, 8);
            generated[i] = oneKey;
        }
        return generated;
    }

    private List<byte[]> takeKeys(byte[][] keys, int toTake) {
        List<byte[]> result = new ArrayList<byte[]>();
        for (int i = 0; i < toTake; i++) {
            // int chosenIndex = randomGenerator.nextInt(keys.length);
            int chosenIndex = i;
            result.add(keys[chosenIndex]);
        }
        return result;
    }
}
