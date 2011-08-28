package com.unister.semweb.sdrum.storable;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.unister.semweb.sdrum.file.HeaderIndexFile;
import com.unister.semweb.sdrum.utils.KeyUtils;

/**
 * This class is a DummyObject, copied from LinkData, and for test purpose only. represents data, which is linked to a
 * url. It extends {@link AbstractKVStorable} to be storable in {@link HeaderIndexFile}s. An object of this class needs
 * 26 bytes in
 * Byte-Representation. The first 8 byte of this
 * representation belongs to the key. <br>
 * <br>
 * <code>
 * ------------------------------ 26 bytes --------------------------<br>
 * key ... | relevanceScore | parentCount | urlPosition | timestamp |<br>
 * 8 bytes | 2 bytes ...... | 4 bytes ... | 8 bytes ... | 4 bytes . |<br>
 * ------------------------------------------------------------------<br>
 * </code><br>
 * <br>
 * Use the methods <code>initFromByteBuffer(...)</code> and <code>toByteBuffer(...)</code> to handle the byte-streams
 * stored in {@link HeaderIndexFile}.
 * 
 * @author n.thieme, m.gleditzsch
 */
public class DummyKVStorable extends AbstractKVStorable<DummyKVStorable> implements Comparable<DummyKVStorable> {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * the size the object needs in byte, if we want to write it in a byte-array. If you make changes to the functions
     * <code>initFromByteBuffer(...)</code> and <code>toByteBuffer(...)</code>
     */
    public final int byteBufferSize = 26;
    public final int keySize = 8;

    /** reflects a reference data in minutes. (2011-01-01 0:0) */
    private static final long START_TIME_IN_MINUTES = 21562500L;

    /** relevance of the object [0,1]. Stored as char to save memory and CPU. Remember: the precision is 1/65535 */
    private char relevanceScore;

    /** how many parents has the object (in store) */
    private int parentCount;

    /** The file pointer to the url file to link to the corresponding url. */
    private long urlPosition;

    /** when have we crawled the object last, time in minutes till <code>START_TIME_IN_MINUTES</code> */
    private int timestamp;

    /**
     * Instantiates an empty {@link DummyKVStorable}-Object.
     * 
     * @param byteBufferSize
     */
    public DummyKVStorable() {
        this.relevanceScore = 0;
        this.key = new byte[keySize];
    }

    /**
     * Instantiates a {@link DummyKVStorable}-Object by the given ByteBuffer.
     * 
     * @param ByteBuffer
     */
    public DummyKVStorable(ByteBuffer bytebuffer) {
        this.key = new byte[keySize];
        this.initFromByteBuffer(bytebuffer);
    }

    @Override
    public int getByteBufferSize() {
        return byteBufferSize;
    }

    /**
     * returns the relevance-score of this LinkData-Object
     * 
     * @return double
     */
    public double getRelevanceScore() {
        // int numericValue = Character.getNumericValue(relevanceScore);
        double converted = relevanceScore / 65535d;
        return converted;
    }

    /**
     * sets the new relevance score of this LinkData-Object. If you try to set a negative value, it will be set to
     * zero.
     * 
     * @param double
     */
    public void setRelevanceScore(double relevanceScore) {
        if (relevanceScore < 0) {
            this.relevanceScore = 0;
        } else {
            this.relevanceScore = (char) Math.floor(relevanceScore * 65535);
        }
    }

    /**
     * sets the new relevance score of this LinkData-Object.
     * 
     * @param char
     */
    private void setRelevanceScore(char relevanceScore) {
        this.relevanceScore = relevanceScore;
    }

    /**
     * returns the number of parents this LinkData-Object has
     * 
     * @return int
     */
    public int getParentCount() {
        return parentCount;
    }

    /**
     * sets the number of parents
     * 
     * @param int
     */
    public void setParentCount(int parentCount) {
        this.parentCount = parentCount;
    }

    /**
     * Gets the position of the url in an url-file (mostly a {@link HeaderFile} ).
     */
    public long getUrlPosition() {
        return urlPosition;
    }

    /**
     * Sets the position of the url within an url-file (mostly a {@link HeaderFile}).
     */
    public void setUrlPosition(long urlPosition) {
        this.urlPosition = urlPosition;
    }

    /** returns the TimeStamp of this {@link DummyKVStorable}-Object */
    public long getTimestamp() {
        return (START_TIME_IN_MINUTES + timestamp) * 60 * 1000;
    }

    /**
     * sets the TimeStamp for this {@link DummyKVStorable}-Object
     * 
     * @param long
     */
    public void setTimestamp(long timestamp) {
        long inMinutes = timestamp / 1000 / 60;
        this.timestamp = (int) (inMinutes - START_TIME_IN_MINUTES);
    }

    /**
     * It is used for building the LinkData from a ByteBuffer. In this case the
     * <code>timestamp<code> must be set directly without computation.
     */
    private void setTimestamp(int timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * Compares this object to the given {@link DummyKVStorable}-Object on the key.
     */
    public int compareTo(DummyKVStorable o) {
        return KeyUtils.compareKey(this.key, o.key);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DummyKVStorable) {
            DummyKVStorable toCompare = (DummyKVStorable) obj;
            if (KeyUtils.compareKey(key, toCompare.key) != 0)
                return false;
            if (relevanceScore != toCompare.relevanceScore)
                return false;
            if (parentCount != toCompare.parentCount)
                return false;
            if (urlPosition != toCompare.urlPosition)
                return false;
            if (timestamp != toCompare.timestamp)
                return false;
            return true;
        }
        return false;
    }

    /**
     * Merges this {@link DummyKVStorable}-object with the given {@link DummyKVStorable} -object and returns a new
     * merged one
     * 
     * @param elementToMerge
     * @return
     */
    @Override
    public DummyKVStorable merge(DummyKVStorable element) {
        DummyKVStorable newElement = new DummyKVStorable();

        /*
         * dont't change this calculation without talking to the responsible developer
         */
        double thisRS = getRelevanceScore();
        double otherRS = element.getRelevanceScore();

        double newRelevanceScore = 0;
        if (thisRS <= 0 || otherRS <= 0) {
            newRelevanceScore = Math.max(otherRS, thisRS);
            if (newRelevanceScore < 0) {
                newRelevanceScore = 0;
            }
        } else {
            newRelevanceScore = ((thisRS * this.parentCount) + (otherRS * element.parentCount))
                    / (this.parentCount + element.parentCount);
        }

        newElement.setKey(this.key);
        newElement.setRelevanceScore(newRelevanceScore);
        newElement.setParentCount(this.parentCount + element.parentCount);
        newElement.setUrlPosition(this.urlPosition);
        newElement.setTimestamp(Math.max(this.getTimestamp(), element.getTimestamp()));
        return newElement;
    }

    /**
     * Updates this {@link DummyKVStorable}-object with values from the given {@link DummyKVStorable}-object. Only
     * values which are
     * not 0 will be updated.
     * 
     * @param elementToMerge
     */
    @Override
    public void update(DummyKVStorable element) {
        if (element.relevanceScore != 0) {
            this.relevanceScore = element.relevanceScore;
        }
        if (element.parentCount != 0) {
            this.parentCount = element.parentCount;
        }
        if (element.urlPosition != 0) {
            this.urlPosition = element.urlPosition;
        }
        if (element.urlPosition != 0) {
            this.timestamp = element.timestamp;
        }
    }

    @Override
    public String toString() {
        return "One link date: fingerprint = " + Arrays.toString(key) + ", relevanceScore = " + getRelevanceScore()
                + ", parent count = " + parentCount + ", urlPoisition = " + urlPosition + ", timestamp = "
                + getTimestamp();
    }

    @Override
    public void initFromByteBuffer(ByteBuffer bytebuffer) {
        bytebuffer.position(0);
        bytebuffer.get(key);
        setRelevanceScore(bytebuffer.getChar());
        setParentCount(bytebuffer.getInt());
        urlPosition = bytebuffer.getLong();
        setTimestamp(bytebuffer.getInt());
    }

    @Override
    public DummyKVStorable fromByteBuffer(ByteBuffer bytebuffer) {
        return new DummyKVStorable(bytebuffer);
    }

    /**
     * generates a {@link ByteBuffer} from the object. The {@link ByteBuffer} .flip() method was executed.
     */
    @Override
    public ByteBuffer toByteBuffer() {
        ByteBuffer bytebuffer = ByteBuffer.allocate(byteBufferSize);
        bytebuffer.put(key).putChar(relevanceScore).putInt(parentCount).putLong(urlPosition).putInt(timestamp);
        bytebuffer.flip();
        return bytebuffer;
    }

    @Override
    public DummyKVStorable clone() throws CloneNotSupportedException {
        ByteBuffer bytebuffer = this.toByteBuffer();
        return fromByteBuffer(bytebuffer);
    }
}
