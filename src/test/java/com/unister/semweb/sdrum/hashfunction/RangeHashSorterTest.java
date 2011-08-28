package com.unister.semweb.sdrum.hashfunction;

import java.util.Arrays;

import junit.framework.Assert;

import org.junit.Test;

import com.unister.semweb.sdrum.bucket.hashfunction.util.RangeHashSorter;
import com.unister.semweb.sdrum.utils.KeyUtils;

/** Tests the sorting machine for the ranges. */
public class RangeHashSorterTest {
    /** The array are already ordered. So this is a test, that the ordering will not damaged. */
    @Test
    public void rightOrderBeforeSearch() {
        System.out.println("################### Preordered Sorting test");
        long ranges[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        byte[][] bRanges = KeyUtils.transformToByteArray(ranges);
        String filenames[] = createFilenames(10);
        int[] sizes = createSizes(10);

        RangeHashSorter sorting = new RangeHashSorter(bRanges, filenames, sizes);
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
        long ranges[] = new long[]{34, 5, 81, 1, 199, 726384};
        byte[][] bRanges = KeyUtils.transformToByteArray(ranges);
        int sizes[] = new int[]{50, 10, 100, 50, 1000, 10000};
        String filenames[] = new String[]{"f1", "f2", "f3", "f4", "f5", "f6"};

        RangeHashSorter sorting = new RangeHashSorter(bRanges, filenames, sizes);
        sorting.quickSort();

        long[] expectedRanges = new long[]{1, 5,  34, 81, 199, 726384};
        byte[][] bExpectedRanges = KeyUtils.transformToByteArray(expectedRanges);
        String[] expectedFilenames = new String[]{"f4", "f2", "f1", "f3", "f5", "f6"};
        
        for (int i = 0; i < bExpectedRanges.length; i++) {
            System.out.println(Arrays.toString(sorting.getRanges()[i]));
            System.out.println(Arrays.toString(bExpectedRanges[i]));
            Assert.assertTrue(Arrays.equals(bExpectedRanges[i], sorting.getRanges()[i]));
        }
        Assert.assertTrue(Arrays.equals(expectedFilenames, sorting.getFilenames()));
    }

    /** Creates an array of sizes from 0 to <code>till</code>. */
    public int[] createSizes(int till) {
        int[] result = new int[till];
        for (int i = 0; i < till; i++) {
            result[i] = i;
        }
        return result;
    }

    /** Creates an array of ranges from 0 to <code>till</code>. */
    public long[] createRanges(int till) {
        long[] result = new long[till];
        for (int i = 0; i < till; i++) {
            result[i] = i;
        }
        return result;
    }

    /** Creates an array of filenames from "Filename0" to "Filename<code>till</code>". */
    public String[] createFilenames(int till) {
        String[] result = new String[till];
        for (int i = 0; i < till; i++) {
            result[i] = "Filename" + i;
        }
        return result;
    }
}
