package com.unister.semweb.sdrum.keyutils;

import junit.framework.Assert;

import org.junit.Test;

import com.unister.semweb.sdrum.utils.KeyUtils;

public class CompareMethod {
    @Test
    public void equalKeysZero() {
        byte[] key1 = createKey(0, 0, 0, 0);
        byte[] key2 = createKey(0, 0, 0, 0);

        Assert.assertEquals(0, KeyUtils.compareKey(key1, key2));
    }

    @Test
    public void equalKey() {
        byte[] key1 = createKey(0, 0, 0, 0, 69, 87, -113, 126);
        byte[] key2 = createKey(0, 0, 0, 50, 45, 1, -9, 117);

        if (KeyUtils.compareKey(key1, key2) != 0) {
            Assert.assertTrue(true);
        } else {
            Assert.fail();
        }
    }

    @Test
    public void unequalKey2() {
        byte[] key1 = createKey(126, -113, 87, 69, 0, 0, 0, 0);
        byte[] key2 = createKey(117, -9, 1, 45, 50, 0, 0, 0);

        if (KeyUtils.compareKey(key1, key2) != 0) {
            Assert.assertTrue(true);
        } else {
            Assert.fail();
        }
    }

    @Test
    public void unequalKey3() {
        byte[] key1 = createKey(6, 36, 58, -20, 0, 0, 0, 0);
        byte[] key2 = createKey(-112, -26, 122, -60, 0, 0, 0, 0);

        if (KeyUtils.compareKey(key1, key2) == 1) {
            Assert.assertTrue(true);
        } else {
            Assert.fail();
        }

    }

    @Test
    public void unequalKey4() {
        byte[] key1 = createKey(0, 0, 0, 0, 69, 87, -113, 126);
        byte[] key2 = createKey(0, 0, 0, 50, 45, 1, -9, 117);

        if (KeyUtils.compareKey(key1, key2) == -1) {
            Assert.assertTrue(true);
        } else {
            Assert.fail();
        }
    }

    @Test
    public void unequalKey5() {
        byte[] key1 = createKey(34, -128, 15, -100, 0, 0, 0, 0);
        byte[] key2 = createKey(34, -122, 20, -82, 0, 0, 0, 0);

        if (KeyUtils.compareKey(key1, key2) == -1) {
            Assert.assertTrue(true);
        } else {
            Assert.fail();
        }
    }

    @Test
    public void unequalKey6() {
        byte[] key1 = createKey(-9, -74, -117, 87, 0, 0, 0, 0);
        byte[] key2 = createKey(-9, -74, -121, 23, 0, 0, 0, 0, 41, -5, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, -72, 35, -2, -74,
                -5, 124, 0, 0, 0, 0, 0);
        if (KeyUtils.compareKey(key1, key2) == 1) {
            Assert.assertTrue(true);
        } else {
            Assert.fail();
        }
    }
    
    @Test
    public void lessKey() {
        byte[] key1 = createKey(-7, 0, 0, 0, 0, 0, 0, 0);
        byte[] key2 = createKey(8, 0, 0, 0, 0, 0, 0, 0);
        
        if (KeyUtils.compareKey(key1, key2) == 1) {
            Assert.assertTrue(true);
        } else {
            Assert.fail();
        }
    }

    private byte[] createKey(int... bytes) {
        byte[] key = new byte[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            key[i] = (byte) bytes[i];
        }
        return key;
    }

    // 126 -113 87 69 0 0 0 0 (this), 117 -9 1 45 50 0 0 0 (element)

    // -101 -82 73 5 0 0 0 0 (this), 84 30 -10 -65 0 0 0 0
    // 26 36 58 -20 0 0 0 0 (this), -112 -26 122 -60 0 0 0 0 (element)

}
