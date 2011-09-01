package com.unister.semweb.sdrum.api;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.unister.semweb.sdrum.api.SDRUM.AccessMode;
import com.unister.semweb.sdrum.bucket.hashfunction.AbstractHashFunction;
import com.unister.semweb.sdrum.storable.AbstractKVStorable;

public class SDRUM_API {
    private static final Logger log = LoggerFactory.getLogger(SDRUM.class);
    public static final String CONFIG_FILE = "_sdrum.cfg";

    /**
     * This method creates a new SDRUM object.<br/>
     * If the given directory doesn't exist, it will be created.<br/>
     * If the given directory already exists an {@link IOException} will be thrown.
     * 
     * @param databaseDirectory
     *            the path, where to store the database files
     * @param sizeOfMemoryBuckets
     *            the size of each bucket within memory
     * @param preQueueSize
     *            the size of the prequeue
     * @param numberOfSynchronizerThreads
     *            the number of threads used for synchronizing
     * @param hashFunction
     *            the hash function, decides where to store/search elements
     * @throws IOException
     *             is thrown if an error occurs or if the SDRUM already exists
     * @return new {@link SDRUM}-object
     */
    public static <Data extends AbstractKVStorable<Data>> SDRUM<Data> createTable(
            String databaseDirectory,
            int sizeOfMemoryBuckets,
            int preQueueSize,
            int numberOfSynchronizerThreads,
            AbstractHashFunction hashFunction,
            Data prototype) throws IOException {
        File databaseDirectoryFile = new File(databaseDirectory);
        if (databaseDirectoryFile.exists()) {
            throw new IOException("The directory already exist. Can't create a SDRUM.");
        }
        // First we create the directory structure.
        new File(databaseDirectory).mkdirs();

        SDRUM<Data> table = new SDRUM<Data>(
                databaseDirectory,
                sizeOfMemoryBuckets,
                preQueueSize,
                numberOfSynchronizerThreads,
                hashFunction,
                prototype,
                AccessMode.READ_WRITE);

        // We store the configuration parameters within the given configuration file.
        ConfigurationFile<Data> configurationFile = new ConfigurationFile<Data>(
                hashFunction.getNumberOfBuckets(),
                sizeOfMemoryBuckets,
                numberOfSynchronizerThreads,
                preQueueSize,
                databaseDirectory,
                hashFunction,
                prototype);
        configurationFile.writeTo(databaseDirectory + "/" + CONFIG_FILE);
        return table;
    }

    /**
     * This method creates a new SDRUM object. The old SDRUM will be overwritten. <br/>
     * If the given directory doesn't exist, it will be created.<br/>
     * A configuration file will be written to the <code>CONFIG_FILE</code> location.
     * 
     * @param databaseDirectory
     *            the path, where to store the database files
     * @param sizeOfMemoryBuckets
     *            the size of each bucket within memory
     * @param preQueueSize
     *            the size of the prequeue
     * @param numberOfSynchronizerThreads
     *            the number of threads used for synchronizing
     * @param hashFunction
     *            the hash function, decides where to store/search elements
     * @throws IOException
     *             if an error occurs while writing the configuration file
     * @return new {@link SDRUM}-object
     */
    public static <Data extends AbstractKVStorable<Data>> SDRUM<Data> forceCreateTable(
            String databaseDirectory,
            int sizeOfMemoryBuckets,
            int preQueueSize,
            int numberOfSynchronizerThreads,
            AbstractHashFunction hashFunction,
            Data prototype) throws IOException {
        File databaseDirectoryFile = new File(databaseDirectory);
        if (databaseDirectoryFile.exists()) {
            deleteDatabaseFilesWithinDirectory(databaseDirectory);
        }
        else
        {
        	databaseDirectoryFile.mkdir();
        }

        // We store the configuration parameters within the given configuration file.
        ConfigurationFile<Data> configurationFile = new ConfigurationFile<Data>(
                hashFunction.getNumberOfBuckets(),
                sizeOfMemoryBuckets,
                numberOfSynchronizerThreads,
                preQueueSize,
                databaseDirectory,
                hashFunction,
                prototype);
        configurationFile.writeTo(CONFIG_FILE);

        SDRUM<Data> table = new SDRUM<Data>(
                databaseDirectory,
                sizeOfMemoryBuckets,
                preQueueSize,
                numberOfSynchronizerThreads,
                hashFunction,
                prototype,
                AccessMode.READ_WRITE);
        return table;
    }

    /**
     * All database files (extension "*.db") in the given directory will be deleted.
     * 
     * @param databaseDirectory
     *            directory of database files that will be deleted
     */
    private static void deleteDatabaseFilesWithinDirectory(String databaseDirectory) {
        File databaseDirectoryFile = new File(databaseDirectory);
        Collection<File> toDelete = FileUtils.listFiles(databaseDirectoryFile, new String[] { "*.db" }, false);
        // TODO: dont forget to delete the config file
        for (File oneFile : toDelete) {
            oneFile.delete();
        }
    }

    /**
     * Checks if the given databaseDirecotry could be a SDRUM table
     * 
     * @param databaseDirectory
     * @return boolean
     */
    public static <Data extends AbstractKVStorable<Data>> boolean tableExists(String databaseDirectory) {
        File f = new File(databaseDirectory + "/" + CONFIG_FILE);
        return f.exists();
    }

    /**
     * Opens an existing table. Only the database configuration file and the access are needed. All other information
     * are loaded from the configuration file.
     * 
     * @param databaseDirectory
     *            the folder of the SDRUM to open
     * @param accessMode
     *            the AccessMode, how to access the SDRUM
     * @param prototype
     *            a prototype of the elments stored in this drum
     * @return the table
     */
    public static <Data extends AbstractKVStorable<Data>> SDRUM<Data> openTable(
            String databaseDirectory,
            AccessMode accessMode,
            Data prototype)
            throws IOException, ClassNotFoundException {

        ConfigurationFile<Data> configFile = ConfigurationFile.readFrom(databaseDirectory + "/" + CONFIG_FILE);

        if (configFile.getPrototype().getByteBufferSize() != prototype.getByteBufferSize()) {
            log.error("The protoype of the user was different from the prototype stored within the configuration file."
                    + "Size of the user prototype: {}, size of the prototype loaded from configuration file: {}",
                    prototype.getByteBufferSize(), configFile.getPrototype().getByteBufferSize());
            throw new IOException(
                    "The prototypes of the configuration file and the user given prototype were different in size.");
        }

        SDRUM<Data> table = new SDRUM<Data>(
                configFile.getDatabaseDirectory(),
                configFile.getBucketSize(),
                configFile.getPreQueueSize(),
                configFile.getNumberOfSynchronizerThreads(),
                configFile.getHashFunction(),
                configFile.getPrototype(),
                accessMode);
        return table;
    }
}