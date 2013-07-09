package com.unister.semweb.drums.utils;

/**
 * Mathematical helper methods.
 * 
 * @author n.thieme
 * 
 */
public class MathUtils {
    /**
     * Makes an integer division of the given <code>dividend</code> and <code>divisor</code>. The result will be round
     * up if necessary. Example computations:
     * <ul>
     * <li>100 / 2 = 50</li>
     * <li>5 / 2 = 3</li>
     * <li>1 / 2 = 1</li>
     * </ul>
     * */
    public static long divideLongsWithCeiling(long dividend, long divisor) {
        long result = dividend / divisor;
        long remainder = dividend % divisor;
        if (remainder != 0) {
            result++;
        }
        return result;
    }
}
