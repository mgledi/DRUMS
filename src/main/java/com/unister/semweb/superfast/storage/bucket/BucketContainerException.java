package com.unister.semweb.superfast.storage.bucket;

/**
 * This exception is thrown, if the a {@link Bucket} is tried to access, which doesn't exist or an element could not be
 * added eitherway.
 * 
 * @author m.gleditzsch
 * 
 */
public class BucketContainerException extends Exception {
    private static final long serialVersionUID = -7034481737223723755L;

    /**
     * Instantiates a new BucketContainerException with the given message
     * 
     * @param message
     */
    public BucketContainerException(String message) {
        super(message);
    }
}
