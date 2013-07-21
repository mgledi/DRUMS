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
package com.unister.semweb.drums.utils;

/**
 * Mathematical helper methods.
 * 
 * @author Nils Thieme
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
