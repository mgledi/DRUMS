package com.unister.semweb.sdrum;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.unister.semweb.commons.properties.PropertiesFactory;
import com.unister.semweb.sdrum.storable.AbstractKVStorable;

/**
 * @author m.gleditzsch
 */
public class GlobalParameters<Data extends AbstractKVStorable> {
    private static Logger log = LoggerFactory.getLogger(GlobalParameters.class);

    public static AtomicInteger INSTANCE_COUNT = new AtomicInteger(0);
    
    public final int ID;
    
    public String PARAM_FILE;

    /** the size of one chunk to read */
    public long BUCKET_MEMORY;

    /** the size of one chunk to read */
    public long MAX_MEMORY_PER_BUCKET;

    /** the size of one chunk to read */
    public int MEMORY_CHUNK;

    /** the size of one chunk to read */
    public long CHUNKSIZE;

    /** The number of threads used for synchronizing */
    public int NUMBER_OF_SYNCHRONIZER_THREADS = 1;

    public int MIN_ELEMENT_IN_BUCKET_BEFORE_SYNC = 1;

    /** File extension of the database files that store the {@link AbstractKVStorable}. */
    public String linkDataFileExtension = ".db";

    /** the initial size by which the file is enlarged. Will be set by {@link GlobalParameters} */
    public int INITIAL_INCREMENT_SIZE;

    /** the initial size of the file. Will be set by {@link GlobalParameters} */
    public int INITIAL_FILE_SIZE;

    /** The size of the key of the AbstractKVStorable */
    public final int keySize;

    /** The size of the full AbstractKVStorable */
    public final int elementSize;

    private Data prototype;

    /** The maximal time in ms a bucket is held in memory without synchronization attempt */
    public long MAX_BUCKET_STORAGE_TIME;

    GlobalParameters(String paramFile, Data prototype) {
        this.PARAM_FILE = paramFile;
        this.keySize = prototype.key.length;
        this.elementSize = prototype.getByteBufferSize();
        this.prototype = prototype;
        this.ID = GlobalParameters.INSTANCE_COUNT.getAndIncrement();
        initParameters();
    }

    public GlobalParameters(Data prototype) {
        this("sdrum.cfg", prototype);
    }

    /**
     * Returns a clone of the prototype. If you need to access the prototype several times, you should create a pointer
     * on your own instance.
     * 
     * @return clone of the internal prototype
     */
    @SuppressWarnings("unchecked")
    public Data getPrototype() {
        return ((Data) prototype.clone());
    }

    public void initParameters() {
        Properties props = PropertiesFactory.getProperties(PARAM_FILE);

        BUCKET_MEMORY = parseSize(props.getProperty("BUCKET_MEMORY", "1G"));
        MEMORY_CHUNK = (int) parseSize(props.getProperty("MEMORY_CHUNK", "1K"));
        MAX_MEMORY_PER_BUCKET = parseSize(props.getProperty("MAX_MEMORY_PER_BUCKET", "100M"));
        CHUNKSIZE = (int) parseSize(props.getProperty("SYNC_CHUNKSIZE", "100M"));
        NUMBER_OF_SYNCHRONIZER_THREADS = Integer.valueOf(props.getProperty("NUMBER_OF_SYNCHRONIZER_THREADS", "1"));
        MAX_BUCKET_STORAGE_TIME = Long.valueOf(props.getProperty("MAX_BUCKET_STORAGE_TIME", "84000000"));

        INITIAL_FILE_SIZE = (int) parseSize(props.getProperty("INITIAL_FILE_SIZE", "16M"));
        INITIAL_INCREMENT_SIZE = (int) parseSize(props.getProperty("INITIAL_INCREMENT_SIZE", "16M"));
        configToString();
    }

    public void configToString() {
        log.info("----- MEMEORY USAGE -----");
        log.info("BUCKET_MEMORY = {}", BUCKET_MEMORY);
        log.info("MEMORY_CHUNK = {}", MEMORY_CHUNK);
        log.info("MAX_MEMORY_PER_BUCKET = {}", MAX_MEMORY_PER_BUCKET);
        log.info("CHUNKSIZE = {}", CHUNKSIZE);
        log.info("----- HeaderIndexFile -----");
        log.info("INITIAL_FILE_SIZE = {}", INITIAL_FILE_SIZE);
        log.info("INITIAL_INCREMENT_SIZE = {}", INITIAL_INCREMENT_SIZE);
    }

    static Pattern p_mem = Pattern.compile("(\\d+)(K|M|G)");

    public static long parseSize(String s) {
        Matcher m = p_mem.matcher(s);
        if (m.find()) {
            int i = Integer.parseInt(m.group(1));
            char multiplier = m.group(2).charAt(0);
            if (multiplier == 'K') {
                return (long) i * 1024l;
            }
            if (multiplier == 'M') {
                return (long) i * 1024l * 1024l;
            }
            if (multiplier == 'G') {
                return (long) i * 1024l * 1024l * 1024l;
            }
        }
        return -1;
    }
}
