package com.unister.semweb.superfast.storage;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.unister.semweb.superfast.storage.buffer.BufferTest;
import com.unister.semweb.superfast.storage.file.HeaderIndexFileTest;
import com.unister.semweb.superfast.storage.syncronizer.UpdateOnlySynchronizerTest;

/**
 * A test suite for a all tests.
 * 
 * @author n.thieme
 * 
 */
@RunWith(Suite.class)
@SuiteClasses({ BucketContainerTest.class, SynchronizerTest.class, SynchronizerPerformanceTest.class, BufferTest.class,
        LinkDataMergeTest.class, HeaderIndexFileTest.class, UpdateOnlySynchronizerTest.class,
         })
public class AllTests {

}
