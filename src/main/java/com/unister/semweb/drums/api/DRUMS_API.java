package com.unister.semweb.drums.api;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.unister.semweb.drums.GlobalParameters;
import com.unister.semweb.drums.api.DRUMS.AccessMode;
import com.unister.semweb.drums.bucket.hashfunction.AbstractHashFunction;
import com.unister.semweb.drums.storable.AbstractKVStorable;

public class DRUMS_API {
    private static final Logger log = LoggerFactory.getLogger(DRUMS.class);

    /**
     * This method creates a new DRUMS object.<br/>
     * If the given directory doesn't exist, it will be created.<br/>
     * If the given directory already exists an {@link IOException} will be thrown.
     * 
     * @param hashFunction
     *            the hash function, decides where to store/search elements
     * @param gp
     *            pointer to the {@link GlobalParameters} used by the {@link DRUMS} to open
     * @throws IOException
     *             is thrown if an error occurs or if the DRUMS already exists
     * @return new {@link DRUMS}-object
     */
    public static <Data extends AbstractKVStorable> DRUMS<Data> createTable(AbstractHashFunction hashFunction,
            GlobalParameters<Data> gp) throws IOException {
        File databaseDirectoryFile = new File(gp.databaseDirectory);
        if (databaseDirectoryFile.exists()) {
            throw new IOException("The directory already exist. Can't create a DRUMS.");
        }
        // First we create the directory structure.
        new File(gp.databaseDirectory).mkdirs();
        log.info("Created directory {}.", gp.databaseDirectory);
        DRUMS<Data> table = new DRUMS<Data>(hashFunction, AccessMode.READ_WRITE, gp);
        return table;
    }

    /**
     * This method creates a new {@link DRUMS} object. The old {@link DRUMS} will be overwritten. <br/>
     * If the given directory doesn't exist, it will be created.<br/>
     * 
     * @param hashFunction
     *            the hash function, decides where to store/search elements
     * @param gp
     *            pointer to the {@link GlobalParameters} used by the {@link DRUMS} to open
     * @throws IOException
     *             if an error occurs while writing the configuration file
     * @return new {@link DRUMS}-object
     */
    public static <Data extends AbstractKVStorable> DRUMS<Data> forceCreateTable(AbstractHashFunction hashFunction,
            GlobalParameters<Data> gp)
            throws IOException {
        File databaseDirectoryFile = new File(gp.databaseDirectory);
        if (databaseDirectoryFile.exists()) {
            deleteDatabaseFilesWithinDirectory(gp.databaseDirectory);
        } else {
            databaseDirectoryFile.mkdir();
        }

        DRUMS<Data> table = new DRUMS<Data>(hashFunction, AccessMode.READ_WRITE, gp);
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
     * Opens an existing table. Only the database configuration file and the access are needed. All other information
     * are loaded from the configuration file.
     * 
     * @param accessMode
     *            the AccessMode, how to access the DRUMS
     * @param gp
     *            pointer to the {@link GlobalParameters} used by the {@link DRUMS} to open
     * @return the table
     */
    public static <Data extends AbstractKVStorable> DRUMS<Data> openTable(AbstractHashFunction hashFunction,
            AccessMode accessMode, GlobalParameters<Data> gp) {
        DRUMS<Data> table = new DRUMS<Data>(hashFunction, accessMode, gp);
        return table;
    }

    /**
     * Creates or opens the table. If the directory doesn't exists it will be created. If the directory exists only an
     * open will be made.
     * 
     * @param hashFunction
     *            the hash function, decides where to store/search elements
     * @param gp
     *            pointer to the {@link GlobalParameters} used by the {@link DRUMS} to open
     * @return
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public static <Data extends AbstractKVStorable> DRUMS<Data> createOrOpenTable(AbstractHashFunction hashFunction,
            GlobalParameters<Data> gp) throws IOException {

        File databaseDirectoryFile = new File(gp.databaseDirectory);
        DRUMS<Data> drums = null;
        if (databaseDirectoryFile.exists()) {
            drums = openTable(hashFunction, AccessMode.READ_WRITE, gp);
        } else {
            drums = createTable(hashFunction, gp);
        }
        drums.getSyncManager().setMaxBucketStorageTime(gp.MAX_BUCKET_STORAGE_TIME);
        return drums;
    }

}
