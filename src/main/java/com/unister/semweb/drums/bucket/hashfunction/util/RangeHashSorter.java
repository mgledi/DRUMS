package com.unister.semweb.drums.bucket.hashfunction.util;

import java.io.Serializable;

import com.unister.semweb.drums.utils.KeyUtils;

/**
 * Code is copied from com.unister.semweb.superfast.storage.bucket.SortMachine to adapt it for the hash function.
 * <p/>
 * This sort machine takes two arrays with the same number of elements. These arrays correspond to each other, meaning
 * that the value of the ith element of the first array corresponds of the ith element of the second and third array. So
 * all three arrays represents a set of triples.
 * <p/>
 * The sort machine sorts these triples by the <code>ranges</code>.
 * <p/>
 * The usage is as follows:
 * <ul>
 * <li>create a sort machine with the three corresponding arrays</li>
 * <li>call the <code>quicksort</code> method</li>
 * <li>get all arrays by the corresponding getter methods</li>
 * </ul>
 * 
 * @author n.thieme, m.gleditzsch
 */
public class RangeHashSorter implements Serializable {
    /**
     * 
     */
    private static final long serialVersionUID = 5586007520316814952L;

    /** The ranges to sort for. */
    private byte[][] ranges;

    /** The file names. */
    private String[] filenames;

    /** Creates a sorting machine with the three arrays. */
    public RangeHashSorter(byte[][] ranges, String[] filenames) {
        this.ranges = ranges;
        this.filenames = filenames;
    }

    /** Get the ranges. After sorting the ranges are in sorted order. */
    public byte[][] getRanges() {
        return ranges;
    }

    /** Get the filename. After sorting, the ith filename corresponds with the ith filename. */
    public String[] getFilenames() {
        return filenames;
    }

    /**
     * Makes a quicksort for the ranges. If the array is small an insertion sort will be made. Algorithm of O( n*log(n)
     * ) asymptotic upper bound.
     */
    public void quickSort() {
        quickSort(0, ranges.length - 1);
    }

    /**
     * Quicksort for ranges. The bounds specify which part of the array is to be sorted.<br />
     * <br />
     * Algorithm of O( n*log(n) ) asymptotic upper bound. <br />
     * This version of quicksort also allows for bounds to be put in to specify what part of the array will be sorted. <br />
     * The part of the array that lies between <b>left</b> and <b>right</b> is the only part that will be sorted.
     * 
     * @param left
     *            The left boundary of what will be sorted.
     * @param right
     *            The right boundary of what will be sorted.
     */
    public void quickSort(int left, int right) {
        if (left + 10 <= right) {
            int partitionIndex = partition(left, right);
            quickSort(left, partitionIndex - 1);
            quickSort(partitionIndex, right);
        } else {
            // Do an insertion sort on the subarray
            insertionSort(left, right);
        }
    }

    /**
     * Partitions part of an array of ranges. <br />
     * The part of the array between <b>left</b> and <b>right</b> will be partitioned around the value held at
     * ranges[right-1].
     * 
     * @param left
     *            The left bound of the array.
     * @param right
     *            The right bound of the array.
     * @return The index of the pivot after the partition has occured.
     */
    private int partition(int left, int right) {
        int i = left;
        int j = right;
        byte[] pivot = ranges[(left + right) / 2];
        while (i <= j) {
            while (KeyUtils.compareKey(ranges[i], pivot) < 0) {
                i++;
            }
            while (KeyUtils.compareKey(ranges[j], pivot) > 0) {
                j--;
            }

            if (i <= j) {
                swap(i, j);
                i++;
                j--;
            }
        }
        return i;
    }

    /**
     * Internal insertion sort routine for subarrays of ranges that is used by quicksort.
     * 
     * @param left
     *            the left-most index of the subarray.
     * @param right
     *            the right-most index of the subarray.
     */
    private void insertionSort(int left, int right) {
        for (int p = left + 1; p <= right; p++) {
            byte[] tmpRange = ranges[p];
            String tmpFilename = filenames[p];

            int j;
            for (j = p; j > left && KeyUtils.compareKey(tmpRange, ranges[j - 1]) < 0; j--) {
                ranges[j] = ranges[j - 1];
                filenames[j] = filenames[j - 1];
            }
            ranges[j] = tmpRange;
            filenames[j] = tmpFilename;
        }
    }

    /** Swaps the elements of the ith position with the elements of the jth position of all three arrays. */
    public void swap(int i, int j) {
        byte[] ltemp = ranges[i];
        ranges[i] = ranges[j];
        ranges[j] = ltemp;

        String tempFilename = filenames[i];
        filenames[i] = filenames[j];
        filenames[j] = tempFilename;
    }
}
