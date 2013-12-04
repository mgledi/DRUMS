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
package com.unister.semweb.drums.api;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.unister.semweb.drums.DRUMSParameterSet;
import com.unister.semweb.drums.api.DRUMS.AccessMode;
import com.unister.semweb.drums.bucket.hashfunction.AbstractHashFunction;
import com.unister.semweb.drums.storable.AbstractKVStorable;

/**
 * This class provides some factory methods to instantiate {@link DRUMS} by opening or creating a table.
 * 
 * @author Martin Nettling
 * 
 */
public class DRUMSInstantiator {
    private static final Logger logger = LoggerFactory.getLogger(DRUMSInstantiator.class);

    private static final String HASHFUNCTION_FILE = "hashfunction.dat";

    /**
     * This method creates a new DRUMS object.<br/>
     * If the directory in {@link DRUMSParameterSet} doesn't exist, it will be created.<br/>
     * If the directory in {@link DRUMSParameterSet} already exists an {@link IOException} will be thrown.
     * 
     * @param hashFunction
     *            the hash function, decides where to store/search elements
     * @param gp
     *            pointer to the {@link DRUMSParameterSet} used by the {@link DRUMS} to open
     * @return new {@link DRUMS}-object
     * @throws IOException
     */
    public static <Data extends AbstractKVStorable> DRUMS<Data> createTable(AbstractHashFunction hashFunction,
            DRUMSParameterSet<Data> gp) throws IOException {
        File databaseDirectoryFile = new File(gp.DATABASE_DIRECTORY);
        if (databaseDirectoryFile.exists()) {
            throw new IOException("The directory " + databaseDirectoryFile + " already exist. Can't create a DRUMS.");
        }
        // First the directory structure must be created
        new File(gp.DATABASE_DIRECTORY).mkdirs();
        logger.info("Created directory {}.", gp.DATABASE_DIRECTORY);

        gp.store();
        storeHashFunction(gp, hashFunction);

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
     *            pointer to the {@link DRUMSParameterSet} used by the {@link DRUMS} to open
     * @throws IOException
     *             if an error occurs while writing the configuration file
     * @return new {@link DRUMS}-object
     */
    public static <Data extends AbstractKVStorable> DRUMS<Data> forceCreateTable(AbstractHashFunction hashFunction,
            DRUMSParameterSet<Data> gp)
            throws IOException {
        File databaseDirectoryFile = new File(gp.DATABASE_DIRECTORY);
        databaseDirectoryFile.delete();
        return createTable(hashFunction, gp);
    }

    /**
     * Opens an existing table.
     * 
     * @param accessMode
     *            the AccessMode, how to access the DRUMS
     * @param gp
     *            pointer to the {@link DRUMSParameterSet} used by the {@link DRUMS} to open
     * @return the table
     * @throws IOException
     */
    public static <Data extends AbstractKVStorable> DRUMS<Data> openTable(AccessMode accessMode,
            DRUMSParameterSet<Data> gp) throws IOException {
        AbstractHashFunction hashFunction;
        try {
            hashFunction = readHashFunction(gp);
        } catch (ClassNotFoundException e) {
            throw new IOException("Could not load HashFunction from " + gp.DATABASE_DIRECTORY, e);
        }
        return new DRUMS<Data>(hashFunction, accessMode, gp);
    }

    /**
     * Creates or opens the table. If the directory doesn't exists it will be created.
     * 
     * @param hashFunction
     *            the hash function, decides where to store/search elements
     * @param gp
     *            pointer to the {@link DRUMSParameterSet} used by the {@link DRUMS} to open
     * 
     * @see #openTable(AccessMode, DRUMSParameterSet)
     * @see #createTable(AbstractHashFunction, DRUMSParameterSet)
     * 
     * @return a DRUMS instance
     * 
     * @throws IOException
     */
    public static <Data extends AbstractKVStorable> DRUMS<Data> createOrOpenTable(AbstractHashFunction hashFunction,
            DRUMSParameterSet<Data> gp) throws IOException {

        File databaseDirectoryFile = new File(gp.DATABASE_DIRECTORY);
        DRUMS<Data> drums = null;
        if (databaseDirectoryFile.exists()) {
            drums = openTable(AccessMode.READ_WRITE, gp);
        } else {
            drums = createTable(hashFunction, gp);
        }
        return drums;
    }

    private static AbstractHashFunction readHashFunction(DRUMSParameterSet<?> gp) throws IOException,
            ClassNotFoundException {
        FileInputStream fis = new FileInputStream(new File(gp.DATABASE_DIRECTORY + "/" + HASHFUNCTION_FILE));
        ObjectInputStream ois = new ObjectInputStream(fis);
        AbstractHashFunction hashFunction = (AbstractHashFunction) ois.readObject();
        ois.close();
        fis.close();
        return hashFunction;
    }

    private static void storeHashFunction(DRUMSParameterSet<?> gp, AbstractHashFunction hashFunction)
            throws IOException {
        FileOutputStream fos = new FileOutputStream(new File(gp.DATABASE_DIRECTORY + "/" + HASHFUNCTION_FILE));
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        oos.writeObject(hashFunction);
        oos.close();
        fos.close();
    }
}
