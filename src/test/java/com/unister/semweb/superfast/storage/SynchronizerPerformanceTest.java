package com.unister.semweb.superfast.storage;

import java.io.File;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.unister.semweb.superfast.storage.storable.DummyKVStorable;
import com.unister.semweb.superfast.storage.synchronizer.Synchronizer;


/**
 * Makes a little performance test for the {@link Synchronizer}. 
 * @author n.thieme
 *
 */
public class SynchronizerPerformanceTest {
	private static final Logger log = LoggerFactory.getLogger(SynchronizerPerformanceTest.class);
	private static final String tempDirectory = "/tmp";
	private static final String testFilename = tempDirectory + "/performanceTest1.db";
	
	@Before
	public void initialise() throws Exception {
		File tempFile = new File(testFilename);
		tempFile.delete();
	}
	
	@Test
	public void oneMillionAddChunkSize10EmptyFile() throws Exception {
		makeTestEmptyFile(10);
	}
	
	@Test
	public void oneMillionAddChunkSize50EmptyFile() throws Exception {
		makeTestEmptyFile(50);
	}
	
	@Test
	public void oneMillionAddChunkSize100EmptyFile() throws Exception {
		makeTestEmptyFile(100);
	}
	
	private void makeTestEmptyFile(int chunkSize) throws Exception {
		DummyKVStorable[] testdata = TestUtils.generateTestdata(1000000);
		
		Synchronizer<DummyKVStorable> synchronizer = new Synchronizer<DummyKVStorable>(testFilename, new DummyKVStorable());
		
		long startTime = System.currentTimeMillis();
		synchronizer.upsert(testdata);
		long endTime = System.currentTimeMillis();
		log.info("I need {} milliseconds to write 1.000.000 data with a chunk size of {}.", (endTime - startTime), chunkSize);
	}
	
	@Test
	public void oneMillionAddChunkSize10NonEmptyFile() throws Exception {
		makeTestNonEmptyFile(1000000, 1000000, 10);
	}
	
	@Test
	public void oneMillionAddChunkSize50NonEmptyFile() throws Exception {
		makeTestNonEmptyFile(1000000, 1000000, 50);
	}
	
	@Test
	public void oneMillionAddChunkSize100NonEmptyFile() throws Exception {
		makeTestNonEmptyFile(1000000, 1000000, 100);
	}
	
	private void makeTestNonEmptyFile(int baseNumberOfDates, int numberOfAdditionalDates, int chunkSize) throws Exception {
		DummyKVStorable[] testdata = TestUtils.generateTestdata(baseNumberOfDates);
		Synchronizer<DummyKVStorable> synchronizer = new Synchronizer<DummyKVStorable>(testFilename, new DummyKVStorable());
		synchronizer.upsert(testdata);
		
		DummyKVStorable[] additionalInsert = TestUtils.generateTestdata(numberOfAdditionalDates);
		long startTime = System.currentTimeMillis();
		synchronizer.upsert(additionalInsert);
		long endTime = System.currentTimeMillis();
		log.info("I need {} milliseconds to write {} data with a chunk size of {}. There were {} entries already in the file.", new Object[]{(endTime - startTime), numberOfAdditionalDates, chunkSize, baseNumberOfDates});
	}
}
