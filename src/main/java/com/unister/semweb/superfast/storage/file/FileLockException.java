package com.unister.semweb.superfast.storage.file;

/**
 * This class extends the {@link Exception}-class. A {@link FileLockException} should be thrown if a File is locked in
 * some way.
 * 
 * @author m.gleditzsch
 * 
 */
public class FileLockException extends Exception {
    private static final long serialVersionUID = 1L;

    /**
     * This method constructs a new {@link FileLockException}
     * 
     * @param String
     *            message, the message to throw
     */
    public FileLockException(String message) {
        super(message);
    }
}
