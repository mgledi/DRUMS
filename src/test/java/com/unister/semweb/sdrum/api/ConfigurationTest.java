package com.unister.semweb.sdrum.api;

import junit.framework.Assert;

import org.junit.Test;

import com.unister.semweb.sdrum.bucket.hashfunction.AbstractHashFunction;
import com.unister.semweb.sdrum.bucket.hashfunction.FirstBitHashFunction;
import com.unister.semweb.sdrum.storable.DummyKVStorable;
import com.unister.semweb.sdrum.utils.KeyUtils;

/**
 * Tests the configuration file writing and reading.
 * 
 * @author n.thieme
 */
public class ConfigurationTest {
    private static final String propertyFile = "/tmp/config.properties";

    /**
     * Makes a write test.
     * 
     * @throws Exception
     */
    @Test
    public void configurationFileTest() throws Exception {
        ConfigurationFile<DummyKVStorable> file = new ConfigurationFile<DummyKVStorable>(100, 10000, 12, 10000,
                "/data/tmp/db", new FirstBitHashFunction(100), new DummyKVStorable());
        file.writeTo(propertyFile);
    }

    /** Writes and reads the configuration. */
    @Test
    public void writeAndReadTest() throws Exception {
        DummyKVStorable prototype = new DummyKVStorable();
        prototype.setKey(KeyUtils.transformFromLong(10));

        ConfigurationFile<DummyKVStorable> file = new ConfigurationFile<DummyKVStorable>(100, 10000, 12, 10000,
                "/data/tmp/db", new FirstBitHashFunction(100), prototype);
        file.writeTo(propertyFile);

        ConfigurationFile<DummyKVStorable> readConfig = ConfigurationFile.readFrom(propertyFile);
        Assert.assertEquals(100, readConfig.getNumberOfBuckets());
        Assert.assertEquals(10000, readConfig.getBucketSize());
        Assert.assertEquals(12, readConfig.getNumberOfSynchronizerThreads());
        Assert.assertEquals(10000, readConfig.getPreQueueSize());
        Assert.assertEquals("/data/tmp/db", readConfig.getDatabaseDirectory());

        AbstractHashFunction hashFunction = readConfig.getHashFunction();
        DummyKVStorable readPrototype = readConfig.getPrototype();
        Assert.assertEquals(10, readPrototype.key);
    }
}
