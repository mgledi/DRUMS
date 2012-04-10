package com.unister.semweb.sdrum.api;

import java.io.FileWriter;
import java.io.IOException;

import net.iharder.Base64;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.unister.semweb.sdrum.bucket.hashfunction.AbstractHashFunction;
import com.unister.semweb.sdrum.storable.AbstractKVStorable;

/** Represents a configuration for the DRUM. */
public class ConfigurationFile<T extends AbstractKVStorable<T>> {
    private static final Logger log = LoggerFactory.getLogger(ConfigurationFile.class);

    private int numberOfBuckets;
    private int numberOfSynchronizerThreads;

    private String databaseDirectory;

    private AbstractHashFunction hashFunction;
    private T prototype;

    /**
     * Creates a configuration file.
     * 
     * @param numberOfBuckets
     * @param numberOfSynchronizerThreads
     * @param databaseDirectory
     * @param hashFunction
     * @param prototype
     */
    public ConfigurationFile(int numberOfBuckets, int numberOfSynchronizerThreads,
            String databaseDirectory, AbstractHashFunction hashFunction, T prototype) {
        this.numberOfBuckets = numberOfBuckets;
        this.numberOfSynchronizerThreads = numberOfSynchronizerThreads;
        this.databaseDirectory = databaseDirectory;
        this.hashFunction = hashFunction;
        this.prototype = prototype;
    }

    /** Writes this configuration to disk as a property file. */
    public void writeTo(String configurationFilename) throws IOException {
        try {
            PropertiesConfiguration propertyConfiguration = new PropertiesConfiguration();
            propertyConfiguration.addProperty("numberOfBuckets", numberOfBuckets);
            propertyConfiguration.addProperty("numberOfSynchronizerThreads", numberOfSynchronizerThreads);
            propertyConfiguration.addProperty("databaseDirectory", databaseDirectory);

            String serializedHashFunction = Base64.encodeObject(hashFunction);
            propertyConfiguration.addProperty("hashFunction", serializedHashFunction);

            String serializedPrototype = Base64.encodeObject(prototype);
            propertyConfiguration.addProperty("prototype", serializedPrototype);

            propertyConfiguration.save(new FileWriter(configurationFilename));
        } catch (ConfigurationException ex) {
            throw new IOException(ex);
        }
    }

    /**
     * Loads the configuration from the given configuration file.
     * 
     * @param <Data>
     * @param configurationFilename
     * @return
     * @throws IOException
     * @throws ClassNotFoundException
     */
    @SuppressWarnings("unchecked")
    public static <Data extends AbstractKVStorable<Data>> ConfigurationFile<Data> readFrom(String configurationFilename)
            throws IOException, ClassNotFoundException {
        ConfigurationFile<Data> result = null;
        try {
            PropertiesConfiguration propertiesConfiguration = new PropertiesConfiguration(configurationFilename);
            int numberOfBuckets = propertiesConfiguration.getInt("numberOfBuckets");
            int numberOfSynchronizerThreads = propertiesConfiguration.getInt("numberOfSynchronizerThreads");
            String readDatabaseDirectory = propertiesConfiguration.getString("databaseDirectory");
            String serialisedHashFunction = propertiesConfiguration.getString("hashFunction");
            String serialisedPrototype = propertiesConfiguration.getString("prototype");

            AbstractHashFunction hashFunction = (AbstractHashFunction) Base64.decodeToObject(serialisedHashFunction);
            Data prototype = (Data) Base64.decodeToObject(serialisedPrototype);

            result = new ConfigurationFile<Data>(numberOfBuckets, numberOfSynchronizerThreads, readDatabaseDirectory,
                    hashFunction, prototype);
        } catch (ConfigurationException ex) {
            throw new IOException(ex);
        }
        return result;

    }

    public int getNumberOfBuckets() {
        return numberOfBuckets;
    }


    public int getNumberOfSynchronizerThreads() {
        return numberOfSynchronizerThreads;
    }

    public String getDatabaseDirectory() {
        return databaseDirectory;
    }

    public AbstractHashFunction getHashFunction() {
        return hashFunction;
    }

    public T getPrototype() {
        return prototype;
    }
}
