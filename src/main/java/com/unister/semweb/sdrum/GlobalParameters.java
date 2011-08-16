package com.unister.semweb.sdrum;

import com.unister.semweb.sdrum.storable.AbstractKVStorable;

/**
 * This class stores global used parameters. Don't change them. 
 * 
 * @author m.gleditzsch
 */
public class GlobalParameters {

    /** the size of one chunk to read */
    public static final long CHUNKSIZE = 2048 * 1024; // 2048 KB

    /** File extension of the database files that store the {@link AbstractKVStorable}. */
    public static final String linkDataFileExtension = ".db";
}
