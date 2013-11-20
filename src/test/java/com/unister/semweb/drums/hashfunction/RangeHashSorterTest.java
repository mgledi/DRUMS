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
package com.unister.semweb.drums.hashfunction;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import com.unister.semweb.drums.bucket.hashfunction.util.RangeHashSorter;
import com.unister.semweb.drums.utils.KeyUtils;

/**
 * This Class tests the sorting machine for the ranges.
 * 
 * @author Martin Nettling
 */
public class RangeHashSorterTest {
    
    /** The array is already sorted. So this is a test, that the ordering will not changed. */
    @Test
    public void rightOrderBeforeSearch() {
        System.out.println("################### Preordered Sorting test");
        long ranges[] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        byte[][] bRanges = KeyUtils.transformToByteArray(ranges);
        String filenames[] = createFilenames(10);

        RangeHashSorter sorting = new RangeHashSorter(bRanges, filenames);
        sorting.quickSort();

        byte[][] sortRanges = sorting.getRanges();
        String sortFilenames[] = sorting.getFilenames();

        long expectedRanges[] = createRanges(10);
        byte[][] bExpectedRanges = KeyUtils.transformToByteArray(expectedRanges);
        String expectedFilenames[] = createFilenames(10);

        for (int i = 0; i < sortRanges.length; i++) {
            Assert.assertTrue(Arrays.equals(bExpectedRanges[i], sortRanges[i]));
        }
        Assert.assertTrue(Arrays.equals(expectedFilenames, sortFilenames));
    }

    /** The order of the ranges is random before sorting. */
    @Test
    public void randomOrderBeforeSearch() {
        System.out.println("################### Random order Sorting test");
        long ranges[] = new long[] { 34, 5, 81, 1, 199, 726384 };
        byte[][] bRanges = KeyUtils.transformToByteArray(ranges);
        String filenames[] = new String[] { "f1", "f2", "f3", "f4", "f5", "f6" };

        RangeHashSorter sorting = new RangeHashSorter(bRanges, filenames);
        sorting.quickSort();

        long[] expectedRanges = new long[] { 1, 5, 34, 81, 199, 726384 };
        byte[][] bExpectedRanges = KeyUtils.transformToByteArray(expectedRanges);
        String[] expectedFilenames = new String[] { "f4", "f2", "f1", "f3", "f5", "f6" };

        Assert.assertArrayEquals(bExpectedRanges, sorting.getRanges());
        Assert.assertArrayEquals(expectedFilenames, sorting.getFilenames());
    }

    /** Creates an array of ranges from 0 to <code>till</code>. */
    private long[] createRanges(int till) {
        long[] result = new long[till];
        for (int i = 0; i < till; i++) {
            result[i] = i;
        }
        return result;
    }

    /** Creates an array of filenames from "Filename0" to "Filename<code>till</code>". */
    private String[] createFilenames(int till) {
        String[] result = new String[till];
        for (int i = 0; i < till; i++) {
            result[i] = "Filename" + i;
        }
        return result;
    }
}
