package com.unister.semweb.drums;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.unister.semweb.drums.storable.AbstractKVStorable;

/**
 * @author m.gleditzsch
 */
public class GlobalParameters<Data extends AbstractKVStorable> {
    private static Logger logger = LoggerFactory.getLogger(GlobalParameters.class);

    public static AtomicInteger INSTANCE_COUNT = new AtomicInteger(0);

    public String databaseDirectory;

    public final int ID;

    public String PARAM_FILE;

    public long BUCKET_MEMORY;

    public long MAX_MEMORY_PER_BUCKET;

    public int MEMORY_CHUNK;

    public long SYNC_CHUNK_SIZE;

    public long INDEX_CHUNK_SIZE;

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

    public GlobalParameters(String paramFile, Data prototype) {
        this.PARAM_FILE = paramFile;
        this.keySize = prototype.key.length;
        this.elementSize = prototype.getByteBufferSize();
        this.prototype = prototype;
        this.ID = GlobalParameters.INSTANCE_COUNT.getAndIncrement();
        initParameters();
    }

    public GlobalParameters(Data prototype) {
        this("drums.properties", prototype);
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
        InputStream fileStream = this.getClass().getClassLoader().getResourceAsStream(PARAM_FILE);
        Properties props = new Properties();
        try {
            props.load(fileStream);
            if (fileStream == null) {
                fileStream = new FileInputStream(PARAM_FILE);
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        databaseDirectory = props.getProperty("DATABASE_DIRECTORY");
        BUCKET_MEMORY = parseSize(props.getProperty("BUCKET_MEMORY", "1G"));
        MEMORY_CHUNK = (int) parseSize(props.getProperty("MEMORY_CHUNK", "10K"));
        MAX_MEMORY_PER_BUCKET = parseSize(props.getProperty("MAX_MEMORY_PER_BUCKET", "100M"));
        SYNC_CHUNK_SIZE = parseSize(props.getProperty("SYNC_CHUNK_SIZE", "2M"));
        INDEX_CHUNK_SIZE = parseSize(props.getProperty("INDEX_CHUNK_SIZE", "32K"));
        INDEX_CHUNK_SIZE = INDEX_CHUNK_SIZE - INDEX_CHUNK_SIZE % prototype.getByteBufferSize(); // estimate exact index
                                                                                                // size
        NUMBER_OF_SYNCHRONIZER_THREADS = Integer.valueOf(props.getProperty("NUMBER_OF_SYNCHRONIZER_THREADS", "1"));
        MAX_BUCKET_STORAGE_TIME = Long.valueOf(props.getProperty("MAX_BUCKET_STORAGE_TIME", "84000000"));
        MIN_ELEMENT_IN_BUCKET_BEFORE_SYNC = Integer
                .valueOf(props.getProperty("MIN_ELEMENT_IN_BUCKET_BEFORE_SYNC", "1"));

        INITIAL_FILE_SIZE = (int) parseSize(props.getProperty("INITIAL_FILE_SIZE", "16M"));
        INITIAL_INCREMENT_SIZE = (int) parseSize(props.getProperty("INITIAL_INCREMENT_SIZE", "16M"));
        configToString();
    }

    public void configToString() {
        logger.info("----- MEMEORY USAGE -----");
        logger.info("BUCKET_MEMORY = {}", BUCKET_MEMORY);
        logger.info("MEMORY_CHUNK = {}", MEMORY_CHUNK);
        logger.info("MAX_MEMORY_PER_BUCKET = {}", MAX_MEMORY_PER_BUCKET);
        logger.info("CHUNKSIZE = {}", SYNC_CHUNK_SIZE);

        logger.info("----- HeaderIndexFile -----");
        logger.info("INITIAL_FILE_SIZE = {}", INITIAL_FILE_SIZE);
        logger.info("INITIAL_INCREMENT_SIZE = {}", INITIAL_INCREMENT_SIZE);
        logger.info("CHUNK_SIZE = {}", INDEX_CHUNK_SIZE);
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
