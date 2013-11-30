/* Copyright (C) 2012-2013 Unister GmbH
 * 
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA. */
package com.unister.semweb.drums;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.unister.semweb.drums.api.DRUMS;
import com.unister.semweb.drums.api.DRUMSException;
import com.unister.semweb.drums.file.HeaderIndexFile;
import com.unister.semweb.drums.storable.AbstractKVStorable;
import com.unister.semweb.drums.storable.GeneralStorable;

/**
 * This class represents all parameters, which are used globally in a DRUMS-Instance. The instance of
 * {@link GlobalParameters} should be available in all internal Objects used by {@link DRUMS}.
 * 
 * @author Martin Nettling
 * @param <Data>
 *            an implementation of {@link AbstractKVStorable}, e.g. {@link GeneralStorable}
 */
public class GlobalParameters<Data extends AbstractKVStorable> {
    /** My private Logger. */
    private static Logger logger = LoggerFactory.getLogger(GlobalParameters.class);
    /**
     * A global count of all instances of {@link GlobalParameters}. This variable is needed if more than one DRUMS will
     * run in one JVM.
     */
    public static AtomicInteger INSTANCE_COUNT = new AtomicInteger(0);

    /** The database directory. All records are written to files in this directory. */
    public String databaseDirectory;
    /** The name of the underlying parameter file. */
    public String PARAMETER_FILE;
    /**
     * a identification number of this parameter set. This variable is needed if more than one DRUMS is instantiated in
     * one JVM.
     */
    public final int ID;
    /** The amount of bytes all buckets are allowed to use. */
    public long BUCKET_MEMORY;
    /**
     * The maximal size of a bucket in bytes. If this size is exceeded, no more elements for this bucket are accepted,
     * until it is synchronized to disk.
     */
    public long MAX_MEMORY_PER_BUCKET;

    /** the size of one chunk in memory */
    public int MEMORY_CHUNK;

    /** the number of retries if a file is locked by another process */
    public int HEADER_FILE_LOCK_RETRY = 100;

    /** The number of bytes, which are read and written at once during synchronization */
    public long SYNC_CHUNK_SIZE;
    /** The size of one chunk in an {@link HeaderIndexFile} */
    public long FILE_CHUNK_SIZE;
    /** The number of threads used for synchronizing. */
    public int NUMBER_OF_SYNCHRONIZER_THREADS = 1;
    /** The minimal number of elements which must be in one bucket, before this bucket is allowed to be synchronized. */
    public int MIN_ELEMENT_IN_BUCKET_BEFORE_SYNC = 1;
    /** File extension of the database files that store the {@link AbstractKVStorable}. */
    public String linkDataFileExtension = ".db";
    /** the initial size by which the file is enlarged, when no more records will fit into the file. */
    public int INITIAL_INCREMENT_SIZE;
    /** the initial size of a {@link HeaderIndexFile}. */
    public int INITIAL_FILE_SIZE;
    /** The size of the key of the implementation of {@link AbstractKVStorable}. */
    public final int keySize;
    /** The number bytes needed by an instance of {@link AbstractKVStorable}. */
    public final int elementSize;
    /** The prototype of the Data of this DRUMS. */
    private Data prototype;

    /** The maximal time in milliseconds a bucket is held in memory without synchronization attempt. */
    public long MAX_BUCKET_STORAGE_TIME;

    /**
     * Initialize global parameters by those from the given parameter-file.
     * 
     * @param paramFile
     *            The name of the parameter-file
     * @param prototype
     *            a prototype of the Data of this DRUMS
     */
    public GlobalParameters(String paramFile, Data prototype) {
        this.PARAMETER_FILE = paramFile;
        this.keySize = prototype.getKey().length;
        this.elementSize = prototype.getSize();
        this.prototype = prototype;
        this.ID = GlobalParameters.INSTANCE_COUNT.getAndIncrement();
        initParameters();
    }

    /**
     * The standard constructor. Loads all parameters from drums.properties
     * 
     * @param prototype
     *            a prototype of the Data of this DRUMS
     */
    public GlobalParameters(Data prototype) {
        this("drums.properties", prototype);
    }

    /**
     * This constructor tries to load an already saved ParameterFile from an existing {@link DRUMS}-table.
     * 
     * @param folder
     *            the directory, where all files can be found
     * @throws DRUMSException
     *             if the given folder is no folder
     */
    public GlobalParameters(File folder) throws DRUMSException {
        if (!folder.isDirectory()) {
            throw new DRUMSException("The given FilePointer (" + folder
                    + ") is no folder. Please specify a folder, which contains a DRUMS-table.");
        }
        keySize = 0;
        elementSize = 0;
        ID=0;
        // TODO: implement
        // load parameter file
        // load prototype
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

    /**
     * Initialises all Parameters.
     */
    public void initParameters() {
        InputStream fileStream = this.getClass().getClassLoader().getResourceAsStream(PARAMETER_FILE);
        Properties props = new Properties();
        try {
            props.load(fileStream);
            if (fileStream == null) {
                fileStream = new FileInputStream(PARAMETER_FILE);
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        databaseDirectory = props.getProperty("DATABASE_DIRECTORY");
        BUCKET_MEMORY = parseSize(props.getProperty("BUCKET_MEMORY", "1G"));
        MEMORY_CHUNK = (int) parseSize(props.getProperty("MEMORY_CHUNK", "10K"));
        MAX_MEMORY_PER_BUCKET = parseSize(props.getProperty("MAX_MEMORY_PER_BUCKET", "100M"));
        SYNC_CHUNK_SIZE = parseSize(props.getProperty("SYNC_CHUNK_SIZE", "2M"));
        FILE_CHUNK_SIZE = parseSize(props.getProperty("FILE_CHUNK_SIZE", "32K"));
        // determine exact index size
        FILE_CHUNK_SIZE = FILE_CHUNK_SIZE - FILE_CHUNK_SIZE % prototype.getSize();
        NUMBER_OF_SYNCHRONIZER_THREADS = Integer.valueOf(props.getProperty("NUMBER_OF_SYNCHRONIZER_THREADS", "1"));
        MAX_BUCKET_STORAGE_TIME = Long.valueOf(props.getProperty("MAX_BUCKET_STORAGE_TIME", "84000000"));
        MIN_ELEMENT_IN_BUCKET_BEFORE_SYNC = Integer
                .valueOf(props.getProperty("MIN_ELEMENT_IN_BUCKET_BEFORE_SYNC", "1"));
        HEADER_FILE_LOCK_RETRY = Integer.valueOf(props.getProperty("HEADER_FILE_LOCK_RETRY", "100"));

        INITIAL_FILE_SIZE = (int) parseSize(props.getProperty("INITIAL_FILE_SIZE", "16M"));
        INITIAL_INCREMENT_SIZE = (int) parseSize(props.getProperty("INITIAL_INCREMENT_SIZE", "16M"));
        configToLogInfo();
    }

    /**
     * Outputs the configuration to the Logger.
     */
    public void configToLogInfo() {
        logger.info("----- MEMEORY USAGE -----");
        logger.info("BUCKET_MEMORY = {}", BUCKET_MEMORY);
        logger.info("MEMORY_CHUNK = {}", MEMORY_CHUNK);
        logger.info("MAX_MEMORY_PER_BUCKET = {}", MAX_MEMORY_PER_BUCKET);
        logger.info("CHUNKSIZE = {}", SYNC_CHUNK_SIZE);

        logger.info("----- HeaderIndexFile -----");
        logger.info("INITIAL_FILE_SIZE = {}", INITIAL_FILE_SIZE);
        logger.info("INITIAL_INCREMENT_SIZE = {}", INITIAL_INCREMENT_SIZE);
        logger.info("CHUNK_SIZE = {}", FILE_CHUNK_SIZE);
    }

    private static Pattern p_mem = Pattern.compile("(\\d+)(K|M|G)");

    /**
     * This methods parses the given String, which should represent a size, to a long. 'K' is interpreted as 1024, 'M'
     * as 1024^2 and 'G' as 1024^3.
     * 
     * @param s
     *            the String to parse
     * @return size in byte. -1 if the String was not parsable.
     */
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
