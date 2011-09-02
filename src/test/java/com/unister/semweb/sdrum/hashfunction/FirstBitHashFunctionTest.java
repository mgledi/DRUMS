package com.unister.semweb.sdrum.hashfunction;

import org.junit.Test;

import com.unister.semweb.sdrum.bucket.hashfunction.FirstBitHashFunction;

public class FirstBitHashFunctionTest {
    @Test
    public void test() {
        FirstBitHashFunction function = new FirstBitHashFunction(4096);
        long maxVal,minVal;
        minVal = this.getMaxValue(1901) + 1;
        maxVal = this.getMaxValue(1902);
        
        minVal = 8561342891631304998L;
        maxVal = 8565846491258675492L;
        
        long diff = maxVal - minVal;
        long stepSize = diff / 20L;
        for(int i=0; i < 20; i++) {
            System.out.println((minVal + stepSize*(i+1)) + " " + "3949_"+i+".db");
        }
        
        System.out.println(minVal +  " | " + maxVal);
        System.out.println(function.getBucketId(maxVal));
//        function.get
    }
    
    /** maps the given key, dependent on the first n bits to an int between 2^0 and 2^n */
    public static long getMaxValue(int bucketId) {
        return ((long)bucketId << (64 - 12))  ;
    }
}
