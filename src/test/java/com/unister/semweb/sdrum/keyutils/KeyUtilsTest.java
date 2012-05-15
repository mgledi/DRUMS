package com.unister.semweb.sdrum.keyutils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Assert;
import org.junit.Test;

import com.unister.semweb.sdrum.utils.KeyUtils;

/**
 * This method tests the {@link KeyUtils}. Those static methods are often used in SDRUM
 * 
 * @author m.gleditzsch, n.thieme
 */
public class KeyUtilsTest {
    @Test
    public void transformByteToLong() {
        byte[] b = KeyUtils.transformFromLong(123456789l, 8);
        assertEquals(123456789l, KeyUtils.transformFromByte(b));
        
        byte[] b2 = KeyUtils.convert(123456789l);
        assertEquals(123456789l, KeyUtils.transformFromByte(b2));
    }
    
    @Test
    public void equalKeysZero() {
        byte[] key1 = intArrayToByteArray(0, 0, 0, 0);
        byte[] key2 = intArrayToByteArray(0, 0, 0, 0);

        assertTrue(KeyUtils.isNull(null));
        assertTrue(KeyUtils.isNull(null,3));
        assertTrue(KeyUtils.isNull(key1));
        assertTrue(KeyUtils.isNull(key1,3));
        assertEquals(0, KeyUtils.compareKey(key1, key2));
    }

    @Test
    public void equalKey() {
        byte[] key1 = intArrayToByteArray(0, 0, 0, 0, 69, 87, -113, 126);
        byte[] key2 = intArrayToByteArray(0, 0, 0, 50, 45, 1, -9, 117);

        if (KeyUtils.compareKey(key1, key2) != 0) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void unequalKey2() {
        byte[] key1 = intArrayToByteArray(126, -113, 87, 69, 0, 0, 0, 0);
        byte[] key2 = intArrayToByteArray(117, -9, 1, 45, 50, 0, 0, 0);

        if (KeyUtils.compareKey(key1, key2) != 0) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void unequalKey3() {
        byte[] key1 = intArrayToByteArray(6, 36, 58, -20, 0, 0, 0, 0);
        byte[] key2 = intArrayToByteArray(-112, -26, 122, -60, 0, 0, 0, 0);

        if (KeyUtils.compareKey(key2, key1) == 1) {
            Assert.assertTrue(true);
        }

    }

    @Test
    public void unequalKey4() {
        byte[] key1 = intArrayToByteArray(0, 0, 0, 0, 69, 87, -113, 126);
        byte[] key2 = intArrayToByteArray(0, 0, 0, 50, 45, 1, -9, 117);

        if (KeyUtils.compareKey(key1, key2) == -1) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void unequalKey5() {
        byte[] key1 = intArrayToByteArray(34, -128, 15, -100, 0, 0, 0, 0);
        byte[] key2 = intArrayToByteArray(34, -122, 20, -82, 0, 0, 0, 0);

        if (KeyUtils.compareKey(key1, key2) == -1) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void unequalKey6() {
        byte[] key1 = intArrayToByteArray(-9, -74, -117, 87, 0, 0, 0, 0);
        byte[] key2 = intArrayToByteArray(-9, -74, -121, 23, 0, 0, 0, 0, 41, -5, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, -72, 35,
                -2, -74,
                -5, 124, 0, 0, 0, 0, 0);
        if (KeyUtils.compareKey(key1, key2) == 1) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void lessKey() {
        byte[] key1 = intArrayToByteArray(-7, 0, 0, 0, 0, 0, 0, 0);
        byte[] key2 = intArrayToByteArray(8, 0, 0, 0, 0, 0, 0, 0);

        if (KeyUtils.compareKey(key1, key2) == 1) {
            Assert.assertTrue(true);
        }
    }

    /**
     * Tests the method with valid data, the array starts with another.
     */
    @Test
    public void validBeginsWith() {
        byte[] beginsWith = new byte[] { 12, 23, 45, 32, 12 };
        byte[] compareWith = new byte[] { 12, 23, 45, 32, 12, 11, 54, 32, 65, 123, 34 };
        Assert.assertTrue(KeyUtils.startsWith(beginsWith, compareWith));
    }

    /**
     * The byte array doesn't starts with the given array.
     */
    @Test
    public void invalidBeginsWith() {
        byte[] beginsWith = new byte[] { 12, 23, 45, 32, 13 };
        byte[] compareWith = new byte[] { 12, 23, 45, 32, 12, 11, 54, 32, 65, 123, 34 };
        Assert.assertFalse(KeyUtils.startsWith(beginsWith, compareWith));
    }

    /**
     * The prefix byte array has less bytes than the compare byte array.
     */
    @Test
    public void invalidSizes() {
        byte[] beginsWith = new byte[] { 12, 23, 45, 32, 12 };
        byte[] compareWith = new byte[] { 12, 23, 45 };
        Assert.assertFalse(KeyUtils.startsWith(beginsWith, compareWith));
    }

    private byte[] intArrayToByteArray(int... bytes) {
        byte[] key = new byte[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            key[i] = (byte) bytes[i];
        }
        return key;
    }

}
