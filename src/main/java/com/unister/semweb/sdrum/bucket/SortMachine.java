package com.unister.semweb.sdrum.bucket;

import com.unister.semweb.sdrum.storable.AbstractKVStorable;
import com.unister.semweb.sdrum.utils.KeyUtils;

/**
 * This class is a container for some static functions to sort special Arrays containing {@link AbstractKVStorable}s.
 * 
 * @author m.gleditzsch
 */
public class SortMachine {

    /**
     * Swaps the element at index i with element at index j
     * 
     * @param A
     *            The array to be sorted.
     * @param i
     *            index of first element
     * @param j
     *            index of second element
     */
    public static void swap(AbstractKVStorable[] A, int i, int j) {
        AbstractKVStorable temp = A[i];
        A[i] = A[j];
        A[j] = temp;
    }

    /**
     * Quicksort for {@link AbstractKVStorable}s. <br />
     * Algorithm of O( n*log(n) ) asymptotic upper bound.
     * 
     * @param A
     *            The array to be sorted.
     * @return A reference to the array that was sorted.
     */
    public static AbstractKVStorable[] quickSort(AbstractKVStorable[] A) {
        quickSort(A, 0, A.length - 1);
        return A;
    }

    /**
     * Quicksort for AbstractKVStorable. The bounds specify which part of the array is to be sorted.<br />
     * <br />
     * Algorithm of O( n*log(n) ) asymptotic upper bound. <br />
     * This version of quicksort also allows for bounds to be put in to specify what part of the array will be sorted. <br />
     * The part of the array that lies between <b>left</b> and <b>right</b> is the only part that will be sorted.
     * 
     * @param A
     *            The array to be sorted.
     * @param left
     *            The left boundary of what will be sorted.
     * @param right
     *            The right boundary of what will be sorted.
     * @return A reference to the array that was sorted.
     */
    public static AbstractKVStorable[] quickSort(AbstractKVStorable[] A, int left, int right) {
        if (left + 10 <= right) {
            int partitionIndex = partition(A, left, right);
            quickSort(A, left, partitionIndex - 1);
            quickSort(A, partitionIndex, right);
        } else {
            // Do an insertion sort on the subarray
            insertionSort(A, left, right);
        }
        return A;
    }

    /**
     * Partitions part of an array of {@link AbstractKVStorable}s. <br />
     * The part of the array between <b>left</b> and <b>right</b> will be partitioned around the value held at
     * A[right-1].
     * 
     * @param A
     *            The array to be partitioned.
     * @param left
     *            The left bound of the array.
     * @param right
     *            The right bound of the array.
     * @return The index of the pivot after the partition has occured.
     */
    private static int partition(AbstractKVStorable A[], int left, int right) {
        int i = left;
        int j = right;
        AbstractKVStorable pivot = A[(left + right) / 2];
        while (i <= j) {
            while (KeyUtils.compareKey(A[i].key, pivot.key) < 0)
                i++;
            while (KeyUtils.compareKey(A[j].key, pivot.key) > 0)
                j--;

            if (i <= j) {
                swap(A, i, j);
                i++;
                j--;
            }
        }
        
        return i;
    }

    /**
     * Internal insertion sort routine for subarrays of {@link AbstractKVStorable}s that is used by quicksort.
     * 
     * @param A
     *            an array of Comparable items.
     * @param left
     *            the left-most index of the subarray.
     * @param right
     *            the right-most index of the subarray.
     */
    private static void insertionSort(AbstractKVStorable[] A, int left, int right) {
        for (int p = left + 1; p <= right; p++) {
            AbstractKVStorable tmp = A[p];
            int j;
            for (j = p; j > left && KeyUtils.compareKey(tmp.key, A[j - 1].key) == -1; j--)
                A[j] = A[j - 1];
            A[j] = tmp;
        }
    }
    
    public static byte[][] quickSort(byte[][] keys)
    {
        quickSort(keys, 0, keys.length - 1);
        return keys;
    }
    
    public static byte[][] quickSort(byte[][] keys, int left, int right) {
        if (left + 10 <= right) {
            int partitionIndex = partition(keys, left, right);
            quickSort(keys, left, partitionIndex - 1);
            quickSort(keys, partitionIndex, right);
        } else {
            // Do an insertion sort on the subarray
            insertionSort(keys, left, right);
        }
        return keys;
    }
 
    private static int partition(byte[][] keys, int left, int right) {
        int i = left;
        int j = right;
        byte[] pivot = keys[(left + right) / 2];
        while (i <= j) {
            while (KeyUtils.compareKey(keys[i], pivot) < 0)
                i++;
            while (KeyUtils.compareKey(keys[j], pivot) > 0)
                j--;

            if (i <= j) {
                swap(keys, i, j);
                i++;
                j--;
            }
        }        
        return i;
    }
    
    public static void swap(byte[][] keys, int i, int j) {
        byte[] temp = keys[i];
        keys[i] = keys[j];
        keys[j] = temp;
    }

    private static void insertionSort(byte[][] keys, int left, int right) {
        for (int p = left + 1; p <= right; p++) {
            byte[] tmp = keys[p];
            int j;
            for (j = p; j > left && KeyUtils.compareKey(tmp, keys[j - 1]) == -1; j--)
                keys[j] = keys[j - 1];
            keys[j] = tmp;
        }
    }


}