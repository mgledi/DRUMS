package com.unister.semweb.sdrum.keyutils;

import junit.framework.Assert;

import org.junit.Test;

import com.unister.semweb.sdrum.utils.KeyUtils;

/**
 * Tests the <code>startWith</code> method of the {@link KeyUtils}.
 * 
 * @author n.thieme
 * 
 */
public class BeginsWith {
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
}
