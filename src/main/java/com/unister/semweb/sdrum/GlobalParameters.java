package com.unister.semweb.sdrum;

import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.unister.semweb.commons.properties.PropertiesFactory;
import com.unister.semweb.sdrum.file.HeaderIndexFile;
import com.unister.semweb.sdrum.storable.AbstractKVStorable;

/**
 * @author m.gleditzsch
 */
public class GlobalParameters {
    private static Logger log = LoggerFactory.getLogger(GlobalParameters.class);
    public static String PARAM_FILE = "sdrum.cfg";

    /** the size of one chunk to read */
    public static long BUCKET_MEMORY;

    /** the size of one chunk to read */
    public static long MAX_MEMORY_PER_BUCKET;

    /** the size of one chunk to read */
    public static int MEMORY_CHUNK;

    /** the size of one chunk to read */
    public static long CHUNKSIZE;

    public static int MIN_ELEMENT_IN_BUCKET_BEFORE_SYNC = 1;

    /** File extension of the database files that store the {@link AbstractKVStorable}. */
    public static String linkDataFileExtension = ".db";

    public static void initParameters() {
        Properties props = PropertiesFactory.getProperties(PARAM_FILE);

        BUCKET_MEMORY = parseSize(props.getProperty("BUCKET_MEMORY", "1G"));
        MEMORY_CHUNK = (int) parseSize(props.getProperty("MEMORY_CHUNK", "1K"));
        MAX_MEMORY_PER_BUCKET = parseSize(props.getProperty("MAX_MEMORY_PER_BUCKET", "100M"));
        CHUNKSIZE = (int) parseSize(props.getProperty("SYNC_CHUNKSIZE", "100M"));

        HeaderIndexFile.INITIAL_FILE_SIZE = (int) parseSize(props.getProperty("INITIAL_FILE_SIZE", "16M"));
        HeaderIndexFile.INITIAL_INCREMENT_SIZE = (int) parseSize(props.getProperty("INITIAL_INCREMENT_SIZE", "16M"));
        configToString();
    }

    public static void configToString() {
        log.info("----- MEMEORY USAGE -----");
        log.info("BUCKET_MEMORY = {}", BUCKET_MEMORY);
        log.info("MEMORY_CHUNK = {}", MEMORY_CHUNK);
        log.info("MAX_MEMORY_PER_BUCKET = {}", MAX_MEMORY_PER_BUCKET);
        log.info("CHUNKSIZE = {}", CHUNKSIZE);
        log.info("----- HeaderIndexFile -----");
        log.info("INITIAL_FILE_SIZE = {}", HeaderIndexFile.INITIAL_FILE_SIZE);
        log.info("INITIAL_INCREMENT_SIZE = {}", HeaderIndexFile.INITIAL_INCREMENT_SIZE);
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
