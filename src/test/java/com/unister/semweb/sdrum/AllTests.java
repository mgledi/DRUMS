package com.unister.semweb.sdrum;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.unister.semweb.sdrum.buffer.BufferTest;
import com.unister.semweb.sdrum.file.HeaderIndexFileTest;
import com.unister.semweb.sdrum.syncronizer.UpdateOnlySynchronizerTest;

/**
 * A test suite for a all tests.
 * 
 * @author n.thieme
 * 
 */
@RunWith(Suite.class)
@SuiteClasses({ BucketContainerTest.class, SynchronizerTest.class, SynchronizerPerformanceTest.class, BufferTest.class,
        LinkDataMergeTest.class, HeaderIndexFileTest.class, UpdateOnlySynchronizerTest.class
         })
public class AllTests {

}
