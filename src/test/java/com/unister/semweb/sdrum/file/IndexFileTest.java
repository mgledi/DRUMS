package com.unister.semweb.sdrum.file;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.unister.semweb.sdrum.TestUtils;
import com.unister.semweb.sdrum.api.SDrumTest;
import com.unister.semweb.sdrum.bucket.hashfunction.AbstractHashFunction;
import com.unister.semweb.sdrum.bucket.hashfunction.RangeHashFunction;
import com.unister.semweb.sdrum.storable.DummyKVStorable;
import com.unister.semweb.sdrum.utils.KeyUtils;


public class IndexFileTest {
    private static final Logger log = LoggerFactory.getLogger(IndexFileTest.class);

    @Before
    public void initialise() throws Exception {
        TestUtils.init();
        FileUtils.deleteQuietly(new File(TestUtils.gp.databaseDirectory));
        new File(TestUtils.gp.databaseDirectory).mkdirs();
        TestUtils.gp = null;

    }
    
    @Test
    public void testCreateAndClose() throws FileLockException, IOException {
        log.info("Create and Close test");
        TestUtils.init();
        TestUtils.gp.index.close();
    }
}
