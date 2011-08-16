package com.unister.semweb.superfast.storage.hashfunction;

import java.util.Arrays;

import junit.framework.Assert;

import org.junit.Test;

import com.unister.semweb.superfast.storage.bucket.hashfunction.util.RangeHashSorter;

/** Tests the sorting machine for the ranges.*/
public class RangeHashSorterTest {
    /** The order of the ranges is sorted before the sorting takes place.*/
    @Test
    public void rightOrderBeforeSearch() {
        long ranges[] = createRanges(10);
        String filenames[] = createFilenames(10);
        
        RangeHashSorter sorting = new RangeHashSorter(ranges, filenames);
        sorting.quickSort();
        
        long sortRanges[] = sorting.getRanges();
        String sortFilenames[] = sorting.getFilenames();
        
        long expectedRanges[] = createRanges(10);
        String expectedFilenames[] = createFilenames(10);
        
        Assert.assertTrue(Arrays.equals(expectedRanges, sortRanges));
        Assert.assertTrue(Arrays.equals(expectedFilenames, sortFilenames));
    }
    
    /** The order of the ranges is random before sorting.*/
    @Test
    public void randomOrderBeforeSearch() {
        long ranges[] = new long[]{34, 12, 81, 0, 199, 726384};
        String filenames[] = new String[]{"f1", "f2", "f3", "f4", "f5", "f6"};
        
        RangeHashSorter sorting = new RangeHashSorter(ranges, filenames);
        sorting.quickSort();
        
        long[] expectedRanges = new long[]{0, 12, 34, 81, 199, 726384};
        String[] expectedFilenames = new String[]{"f4", "f2", "f1", "f3", "f5", "f6"};
        
        Assert.assertTrue(Arrays.equals(expectedRanges, sorting.getRanges()));
        Assert.assertTrue(Arrays.equals(expectedFilenames, sorting.getFilenames()));
        
    }
    
    /** Creates an array of ranges from 0 to <code>till</code>.*/
    public long[] createRanges(int till) {
        long[] result = new long[till];
        for(int i = 0; i < till; i++) {
            result[i] = i;
        }
        return result;
    }
    
    /** Creates an array of filenames from "Filename0" to "Filename<code>till</code>".*/
    public String[] createFilenames(int till) {
        String[] result = new String[till];
        for(int i = 0; i < till; i++) {
            result[i] = "Filename" + i;
        }
        return result;
    }
}
