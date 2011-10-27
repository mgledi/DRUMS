package com.unister.semweb.sdrum.utils;

public class KeySearcher {

    public static int searchFor(byte[][] toSearchIn, byte[] searchFor) {
        int leftIndex = 0;
        int rightIndex = toSearchIn.length - 1;
        int midIndex = (int) Math.floor(rightIndex / 2);

        while (leftIndex < rightIndex) {
            byte[] leftElement = toSearchIn[leftIndex];
            byte[] midElement = toSearchIn[midIndex];
            byte[] rightElement = toSearchIn[rightIndex];

            int compareValueLeft = KeyUtils.compareKey(leftElement, searchFor);
            int compareValueMid = KeyUtils.compareKey(searchFor, midElement);
            int compareValueRight = KeyUtils.compareKey(searchFor, rightElement);

            // Search element is equal to the mid element.
            if (compareValueMid == 0) {
                return midIndex;
            }

            // Search element is equal to the left element
            if (compareValueLeft == 0) {
                return leftIndex;
            }

            // Search element is equal to the right element
            if (compareValueRight == 0) {
                return rightIndex;
            }

            // Search element is less than the mid element
            if (compareValueMid == -1) {
                rightIndex = midIndex - 1;
            }

            // Search element is greater than the mid element
            if (compareValueMid == 1) {
                leftIndex = midIndex + 1;
            }
            midIndex = leftIndex + (int) Math.floor((rightIndex - leftIndex) / 2);
        }
        return -1;
    }
}
