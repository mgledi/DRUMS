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
package com.unister.semweb.drums.bucket.hashfunction.util;

import com.unister.semweb.drums.util.KeyUtils;

/**
 * This class is able to perform an associative sort. It takes two arrays with the same number of elements. These arrays
 * correspond to each other, meaning that the value of the ith element of the first array corresponds of the ith element
 * of the second array.
 * 
 * @author Nils Thieme, Martin Nettling
 */
public class RangeHashSorter {
    /** The ranges to sort for. */
    private byte[][] ranges;

    /** The file names. */
    private String[] filenames;

    /**
     * Creates a sorting machine with the three arrays.
     * 
     * @param ranges
     *            the ranges to sort
     * @param filenames
     *            the filenames to sort depending on the ranges
     */
    public RangeHashSorter(byte[][] ranges, String[] filenames) {
        this.ranges = ranges;
        this.filenames = filenames;
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
    protected void swap(int i, int j) {
        byte[] ltemp = ranges[i];
        ranges[i] = ranges[j];
        ranges[j] = ltemp;

        String tempFilename = filenames[i];
        filenames[i] = filenames[j];
        filenames[j] = tempFilename;
    }
}
