package com.unister.semweb.superfast.storage.bucket.hashfunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.unister.semweb.superfast.storage.GlobalParameters;
import com.unister.semweb.superfast.storage.bucket.BucketContainer;
import com.unister.semweb.superfast.storage.storable.KVStorable;

/**
 * This hashFunction maps an element dependent on its first n bits.
 * 
 * @author m.gleditzsch
 * 
 */
public class FirstBitHashFunction extends AbstractHashFunction {
    private static final long serialVersionUID = 1144011836893651925L;

    /** the logger */
    private static final Logger log = LoggerFactory.getLogger(BucketContainer.class);

    /** maximum allowed buckets */
    private static final int maxBuckets = 65536;

    /** how many bits are used for <code>buckets</code> */
    private int firstUsedBits;

    /**
     * Constructor. Needs only the number of buckets. Scales automatically to the next pow of 2, to estimate the needed
     * number of bits to represent those buckets.
     * 
     * @param int buckets, the number of buckets
     */
    public FirstBitHashFunction(int buckets) {
        this.buckets = nextHighestPowerOfTwo(buckets);
        if (this.buckets != buckets) {
            log.info("Buckets {} was not a power of 2. Using {} instead", buckets, this.buckets);
        }
        if (this.buckets > maxBuckets) {
            this.buckets = maxBuckets;
            log.info("Two much buckets wanted. Using {} instead.", maxBuckets);
        }

        this.firstUsedBits = 0;
        int tmp = this.buckets;
        // TODO Is this right shift correct??? This right shift respect the leading sign of the number!!!
        while ((tmp = tmp >> 1) > 0) {
            firstUsedBits++;
        }
    }

    @Override
    public int getBucketId(long key) {
        return getBucketId(key, firstUsedBits);
    }

    @Override
    public int getBucketId(KVStorable<?> element) {
        return getBucketId(element.getKey());
    }

    /** maps the given key, dependent on the first n bits to an int between 2^0 and 2^n */
    public static int getBucketId(long key, int n) {
        return (int) (key >>> (64 - n));
    }

    /**
     * returns the next highest power of two, or the current value if it's already a power of two or zero
     */
    public static int nextHighestPowerOfTwo(int v) {
        v--;
        v |= v >> 1;
        v |= v >> 2;
        v |= v >> 4;
        v |= v >> 8;
        v |= v >> 16;
        v++;
        return v;
    }

    /**
     * Returns the file name of the given <code>bucketId</code>.
     */
    @Override
    public String getFilename(int bucketId) {
        return new StringBuilder().append(bucketId).append(GlobalParameters.linkDataFileExtension).toString();
    }
}
