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
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.unister.semweb.drums.GlobalParameters;
import com.unister.semweb.drums.api.DRUMS.AccessMode;
import com.unister.semweb.drums.bucket.hashfunction.AbstractHashFunction;
import com.unister.semweb.drums.storable.AbstractKVStorable;

/**
 * This class provides some factory methods to instantiate a DRUMS-table.
 * 
 * @author Martin Nettling
 * 
 */
public class DRUMSInstantiator {
    private static final Logger logger = LoggerFactory.getLogger(DRUMSInstantiator.class);

    /**
     * This method creates a new DRUMS object.<br/>
     * If the directory in {@link GlobalParameters} doesn't exist, it will be created.<br/>
     * If the directory in {@link GlobalParameters} already exists an {@link IOException} will be thrown.
     * 
     * @param hashFunction
     *            the hash function, decides where to store/search elements
     * @param gp
     *            pointer to the {@link GlobalParameters} used by the {@link DRUMS} to open
     * @return new {@link DRUMS}-object
     * @throws IOException 
     */
    public static <Data extends AbstractKVStorable> DRUMS<Data> createTable(AbstractHashFunction hashFunction,
            GlobalParameters<Data> gp) throws IOException {
        File databaseDirectoryFile = new File(gp.databaseDirectory);
        if (databaseDirectoryFile.exists()) {
            throw new IOException("The directory " + databaseDirectoryFile + " already exist. Can't create a DRUMS.");
        }
        // First the directory structure must be created
        new File(gp.databaseDirectory).mkdirs();
        logger.info("Created directory {}.", gp.databaseDirectory);
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
        databaseDirectoryFile.delete();
        return createTable(hashFunction, gp);
    }

    /**
     * Opens an existing table.
     * 
     * @param hashFunction
     *            the hash-function to use
     * 
     * @param accessMode
     *            the AccessMode, how to access the DRUMS
     * @param gp
     *            pointer to the {@link GlobalParameters} used by the {@link DRUMS} to open
     * @return the table
     * @throws IOException 
     */
    public static <Data extends AbstractKVStorable> DRUMS<Data> openTable(AbstractHashFunction hashFunction,
            AccessMode accessMode, GlobalParameters<Data> gp) throws IOException {
        return new DRUMS<Data>(hashFunction, accessMode, gp);
    }

    /**
     * Creates or opens the table. If the directory doesn't exists it will be created.
     * 
     * @param hashFunction
     *            the hash function, decides where to store/search elements
     * @param gp
     *            pointer to the {@link GlobalParameters} used by the {@link DRUMS} to open
     * 
     * @see #openTable(AbstractHashFunction, AccessMode, GlobalParameters)
     * @see #createTable(AbstractHashFunction, GlobalParameters)
     * 
     * @return a DRUMS instance
     * 
     * @throws IOException
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
        return drums;
    }
}
