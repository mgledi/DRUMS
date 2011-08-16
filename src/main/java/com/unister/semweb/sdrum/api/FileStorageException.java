package com.unister.semweb.sdrum.api;

/**
 * Is thrown when an error occurs within the file storage.<br/>
 * There are standard constructor overloaded and implemented.
 * 
 * @author n.thieme
 *
 */
public class FileStorageException extends Exception {
    /**
     * 
     */
    private static final long serialVersionUID = -6381038599235245823L;

    public FileStorageException() {
        super();
    }
    
    public FileStorageException(String message) {
        super(message);
    }
    
    public FileStorageException(String message, Throwable throwable) {
        super(message, throwable);
    }
    
    public FileStorageException(Throwable throwable) {
        super(throwable);
    }
}
