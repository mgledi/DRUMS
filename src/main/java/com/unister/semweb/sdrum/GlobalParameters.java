package com.unister.semweb.sdrum;


import java.util.Properties;

import com.unister.semweb.sdrum.storable.AbstractKVStorable;

/**
 * @author m.gleditzsch
 */
public class GlobalParameters {
    public static String PARAM_FILE = "config.cfg";
    
    /** the size of one chunk to read */
    public static final long BUCKET_MEMORY = 1l*1024l*1024l*1024l; // 1GB

    /** the size of one chunk to read */
    public static final long MAX_MEMORY_PER_BUCKET = 100l*1024l*1024l; // 100MB
    
    /** the size of one chunk to read */
    public static final int MEMORY_CHUNK = 100*1024; // 100KB
    
    /** the size of one chunk to read */
    public static final long CHUNKSIZE = 2048l * 1024l; // 2048 KB

    /** File extension of the database files that store the {@link AbstractKVStorable}. */
    public static final String linkDataFileExtension = ".db";
}
