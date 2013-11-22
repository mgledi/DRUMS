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
package com.unister.semweb.drums.keyutils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Assert;
import org.junit.Test;

import com.unister.semweb.drums.utils.Bytes;
import com.unister.semweb.drums.utils.KeyUtils;

/**
 * This method tests the {@link KeyUtils}. Those static methods are often used in DRUMS
 * 
 * @author Martin Nettling, Nils Thieme
 */
public class KeyUtilsTest {
    
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

    private byte[] intArrayToByteArray(int... bytes) {
        byte[] key = new byte[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            key[i] = (byte) bytes[i];
        }
        return key;
    }

}
