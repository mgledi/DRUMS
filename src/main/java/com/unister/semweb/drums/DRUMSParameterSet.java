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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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
 * {@link DRUMSParameterSet} should be available in all internal Objects used by {@link DRUMS}.
 * 
 * @author Martin Nettling
 * @param <Data>
 *            an implementation of {@link AbstractKVStorable}, e.g. {@link GeneralStorable}
 */
public class DRUMSParameterSet<Data extends AbstractKVStorable> {
    /** My private Logger. */
    private static Logger logger = LoggerFactory.getLogger(DRUMSParameterSet.class);

    private static final String PROTOTYPE_FILE = "prototype.dat";
    private static final String PROPERTY_FILE = "drums.properties";

    /**
     * A global count of all instances of {@link DRUMSParameterSet}. This variable is needed if more than one DRUMS will
     * run in one JVM.
     */
    protected static AtomicInteger INSTANCE_COUNT = new AtomicInteger(0);

    /**
     * a identification number of this parameter set. This variable is needed if more than one DRUMS is instantiated in
     * one JVM.
     */
    public final int instanceID;
    /** File extension of the database files that store the {@link AbstractKVStorable}. */
    public final String linkDataFileExtension = ".db";
    /** The size of the key of the implementation of {@link AbstractKVStorable}. */
    private final int keySize;
    /** The number bytes needed by an instance of {@link AbstractKVStorable}. */
    private final int elementSize;
    /** The prototype of the Data of this DRUMS. */
    private Data prototype;
    /** The name of the underlying parameter file. */
    public String parameterfile;

    // ############################ Parameters which can be manipulated from extern #######################
    /** The database directory. All records are written to files in this directory. */
    public String DATABASE_DIRECTORY;
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
    /** the initial size by which the file is enlarged, when no more records will fit into the file. */
    public int INITIAL_INCREMENT_SIZE;
    /** the initial size of a {@link HeaderIndexFile}. */
    public int INITIAL_FILE_SIZE;
    /** The maximal time in milliseconds a bucket is held in memory without synchronization attempt. */
    public long MAX_BUCKET_STORAGE_TIME;

    /**
     * Initialize global parameters by those from the given parameter-file.
     * 
     * @param paramFile
     *            The name of the parameter-file
     * @param prototype
     *            a prototype of the Data of this DRUMS
     * @throws IOException
     */
    public DRUMSParameterSet(String paramFile, Data prototype) throws IOException {
        this.parameterfile = paramFile;
        this.keySize = prototype.getKey().length;
        this.elementSize = prototype.getSize();
        this.prototype = prototype;
        this.instanceID = DRUMSParameterSet.INSTANCE_COUNT.getAndIncrement();
        initParameters();
    }

    /**
     * The standard constructor. Loads all parameters from drums.properties
     * 
     * @param prototype
     *            a prototype of the Data of this DRUMS
     * @throws IOException
     */
    public DRUMSParameterSet(Data prototype) throws IOException {
        this("drums.properties", prototype);
    }

    /**
     * This constructor tries to load an already saved ParameterFile from an existing {@link DRUMS}-table.
     * 
     * @param folder
     *            the directory, where all files can be found
     * @throws DRUMSException
     *             if the given folder is no folder
     * @throws IOException
     */
    public DRUMSParameterSet(File folder) throws DRUMSException, IOException {
        if (!folder.isDirectory()) {
            throw new DRUMSException("The given FilePointer (" + folder
                    + ") is no folder. Please specify a folder, which contains a DRUMS-table.");
        }
        DATABASE_DIRECTORY = folder.getAbsolutePath();

        // load prototype
        try {
            readPrototype();
        } catch (ClassNotFoundException e) {
            throw new IOException("Could not load prototype from " + DATABASE_DIRECTORY + ".", e);
        }
        this.keySize = prototype.getKey().length;
        this.elementSize = prototype.getSize();

        // load parameter file
        this.parameterfile = getPropertiesFilename();
        this.initParameters();

        this.instanceID = DRUMSParameterSet.INSTANCE_COUNT.getAndIncrement();
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
     * 
     * @throws FileNotFoundException
     */
    private void initParameters() throws FileNotFoundException {
        InputStream fileStream;
        if (new File(parameterfile).exists()) {
            logger.info("Try reading properties directly from {}.", parameterfile);
            fileStream = new FileInputStream(new File(parameterfile));
        } else {
            logger.info("Try reading properties from Resources");
            fileStream = this.getClass().getClassLoader().getResourceAsStream(parameterfile);
        }

        Properties props = new Properties();
        try {
            props.load(fileStream);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        DATABASE_DIRECTORY = props.getProperty("DATABASE_DIRECTORY");
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

    private static Pattern p_mem = Pattern.compile("(\\d+)(K|M|G|)");

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
            long i = Long.parseLong(m.group(1));
            char multiplier = 'B';
            if (m.group(2) != null && m.group(2).length() == 1) {
                multiplier = m.group(2).charAt(0);
            }
            if (multiplier == 'B') {
                return (long) i;
            }
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

    /**
     * @return the properties as {@link Properties}-object
     */
    public Properties getProperties() {
        Properties props = new Properties();
        props.setProperty("DATABASE_DIRECTORY", DATABASE_DIRECTORY + "");
        props.setProperty("BUCKET_MEMORY", BUCKET_MEMORY + "");
        props.setProperty("MEMORY_CHUNK", MEMORY_CHUNK + "");
        props.setProperty("MAX_MEMORY_PER_BUCKET", MAX_MEMORY_PER_BUCKET + "");
        props.setProperty("SYNC_CHUNK_SIZE", SYNC_CHUNK_SIZE + "");
        props.setProperty("FILE_CHUNK_SIZE", FILE_CHUNK_SIZE + "");
        props.setProperty("NUMBER_OF_SYNCHRONIZER_THREADS", NUMBER_OF_SYNCHRONIZER_THREADS + "");
        props.setProperty("MAX_BUCKET_STORAGE_TIME", MAX_BUCKET_STORAGE_TIME + "");
        props.setProperty("MIN_ELEMENT_IN_BUCKET_BEFORE_SYNC", MIN_ELEMENT_IN_BUCKET_BEFORE_SYNC + "");
        props.setProperty("HEADER_FILE_LOCK_RETRY", HEADER_FILE_LOCK_RETRY + "");
        props.setProperty("INITIAL_FILE_SIZE", INITIAL_FILE_SIZE + "");
        props.setProperty("INITIAL_INCREMENT_SIZE", INITIAL_INCREMENT_SIZE + "");
        return props;
    }

    /**
     * @return the size of an element in bytes
     */
    public int getElementSize() {
        return elementSize;
    }

    /**
     * @return the size of the key of an element in bytes
     */
    public int getKeySize() {
        return keySize;
    }

    /**
     * This method stores this {@link DRUMSParameterSet}. Two file are generated. The first file contains the prototype.
     * The second file contains all properties.
     * 
     * @throws IOException
     *             if it was not possible to store all information
     */
    public void store() throws IOException {
        this.storePrototype();
        this.storeProperties();
    }

    private String getPrototypeFilename() {
        return DATABASE_DIRECTORY + "/" + PROTOTYPE_FILE;
    }

    private String getPropertiesFilename() {
        return DATABASE_DIRECTORY + "/" + PROPERTY_FILE;
    }

    private final void readPrototype() throws IOException, ClassNotFoundException {
        FileInputStream fis = new FileInputStream(new File(getPrototypeFilename()));
        ObjectInputStream ois = new ObjectInputStream(fis);
        @SuppressWarnings("unchecked")
        Data tmp = (Data) ois.readObject();
        this.prototype = tmp;
        ois.close();
        fis.close();
    }

    private void storePrototype() throws IOException {
        FileOutputStream fos = new FileOutputStream(new File(getPrototypeFilename()));
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        oos.writeObject(getPrototype());
        oos.close();
        fos.close();
    }

    private void storeProperties() throws IOException {
        FileOutputStream fos = new FileOutputStream(new File(getPropertiesFilename()));
        getProperties().store(fos, "Parameters stored automatically after creating DRUMS.");
        fos.close();
    }
}
