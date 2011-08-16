package com.unister.semweb.superfast.storage;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.unister.semweb.superfast.storage.file.AbstractHeaderFile.AccessMode;
import com.unister.semweb.superfast.storage.file.FileLockException;
import com.unister.semweb.superfast.storage.file.HeaderIndexFile;
import com.unister.semweb.superfast.storage.storable.DummyKVStorable;
import com.unister.semweb.superfast.storage.synchronizer.Synchronizer;


public class SynchronizerTest {
    private static final Logger log = LoggerFactory.getLogger(SynchronizerTest.class);
    private static final String temporaryDirectory = "/tmp";
	private static final String testFilename = temporaryDirectory + "/1.db";
	private static final int numberOfReadChunks = 100;
	
	@Before
	public void initialise() {
		File testFile = new File(testFilename);
		if (testFile.exists()) {
			testFile.delete();
		}
	}

	@Test
	public void addOneEntryInEmptyFile() throws Exception {
	    System.out.println("========= addOneEntryInEmptyFile");
        Synchronizer<DummyKVStorable> synchronizer = new Synchronizer<DummyKVStorable>(testFilename, new DummyKVStorable());
		DummyKVStorable[] toAdd = TestUtils.generateTestdata(1);
		System.out.println(toAdd[0].key);
		synchronizer.upsert(toAdd);
		
		DummyKVStorable[] readLinkData = readInFile(testFilename);
		boolean result = TestUtils.areEqual(toAdd, readLinkData);
		Assert.assertEquals(true, result);
	}
	
	@Test
	public void addSeveralEntriesInEmptyFile() throws Exception {
        System.out.println("========= addSeveralEntriesInEmptyFile");
		DummyKVStorable[] toAdd = TestUtils.generateTestdata(100);
		Arrays.sort(toAdd);

        Synchronizer<DummyKVStorable> synchronizer = new Synchronizer<DummyKVStorable>(testFilename, new DummyKVStorable());
		synchronizer.upsert(toAdd);
		
		DummyKVStorable[] readLinkData = readInFile(testFilename);
		boolean areEqual = TestUtils.areEqual(toAdd, readLinkData);
		Assert.assertEquals(true, areEqual);
	}
	
	@Test
	public void addOneEntryInNonEmptyFileLess() throws Exception {
		DummyKVStorable[] toAdd = TestUtils.generateTestdata(1);
		toAdd[0].setKey(10);

        Synchronizer<DummyKVStorable> synchronizer = new Synchronizer<DummyKVStorable>(testFilename, new DummyKVStorable());
		synchronizer.upsert(toAdd);
		
		DummyKVStorable[] toAddAdditional = TestUtils.generateTestdata(1);
		toAddAdditional[0].setKey(4);
		synchronizer.upsert(toAddAdditional);
		
		DummyKVStorable[] originalData = new DummyKVStorable[]{toAddAdditional[0], toAdd[0]};
		DummyKVStorable[] readLinkData = readInFile(testFilename);
		boolean areEqual = TestUtils.areEqual(originalData, readLinkData);
		Assert.assertEquals(true, areEqual);
	}
	
	@Test
	public void addOneEntryInNonEmptyFileGreater() throws Exception {
		DummyKVStorable[] toAdd = TestUtils.generateTestdata(1);
		toAdd[0].setKey(4);

        Synchronizer<DummyKVStorable> synchronizer = new Synchronizer<DummyKVStorable>(testFilename, new DummyKVStorable());
		synchronizer.upsert(toAdd);
		
		DummyKVStorable[] toAddAdditional = TestUtils.generateTestdata(1);
		toAddAdditional[0].setKey(10);
		synchronizer.upsert(toAddAdditional);
		
		DummyKVStorable[] originalData = new DummyKVStorable[]{toAdd[0], toAddAdditional[0]};
		DummyKVStorable[] readLinkData = readInFile(testFilename);
		boolean areEqual = TestUtils.areEqual(originalData, readLinkData);
		Assert.assertEquals(true, areEqual);
	}
	
	@Test
	public void addSeveralEntriesNonEmptyFile() throws Exception {
		DummyKVStorable[] firstAdd = TestUtils.generateTestdata(100);
		Arrays.sort(firstAdd);

        Synchronizer<DummyKVStorable> synchronizer = new Synchronizer<DummyKVStorable>(testFilename, new DummyKVStorable());
		synchronizer.upsert(firstAdd);
		
		DummyKVStorable[] secondAdd = TestUtils.generateTestdata(67);
		secondAdd = TestUtils.subtract(secondAdd, firstAdd);
		Arrays.sort(secondAdd);
		synchronizer.upsert(secondAdd);
		
		DummyKVStorable[] originalData = ArrayUtils.addAll(firstAdd, secondAdd);
		Arrays.sort(originalData);
				
		DummyKVStorable[] readLinkData = readInFile(testFilename);
		boolean areEqual = TestUtils.areEqual(originalData, readLinkData);
		Assert.assertEquals(true, areEqual);		
	}
	
	@Test
	public void updateElements() throws Exception {
		DummyKVStorable[] toAdd = TestUtils.generateTestdata(10);
		Arrays.sort(toAdd);
		DummyKVStorable toUpdateOriginal = toAdd[3];
		toUpdateOriginal.setTimestamp(System.currentTimeMillis());

        Synchronizer<DummyKVStorable> synchronizer = new Synchronizer<DummyKVStorable>(testFilename, new DummyKVStorable());
		synchronizer.upsert(toAdd);
		
		DummyKVStorable toUpdate = new DummyKVStorable();
		toUpdate.setKey(toUpdateOriginal.getKey());
		toUpdate.setRelevanceScore(1.0);
		toUpdate.setParentCount(1);
		toUpdate.setTimestamp(toUpdateOriginal.getTimestamp());
		
		DummyKVStorable[] toUpdateData = new DummyKVStorable[]{toUpdate};
		synchronizer.upsert(toUpdateData);
		
		DummyKVStorable[] readLinkData = readInFile(testFilename);
		DummyKVStorable searchResult = TestUtils.searchFor(readLinkData, toUpdate.getKey());
		if (searchResult == null) {
			Assert.fail();
		} else {
			DummyKVStorable expectedLinkDate = new DummyKVStorable();
			expectedLinkDate.setKey(toUpdateOriginal.getKey());
			expectedLinkDate.setParentCount(toUpdateOriginal.getParentCount() + 1);
			expectedLinkDate.setRelevanceScore(toUpdateOriginal.getRelevanceScore() * toUpdateOriginal.getParentCount() + toUpdate.getRelevanceScore() / (toUpdateOriginal.getParentCount() + 1));
			
			Assert.assertEquals(toUpdateOriginal.getKey(), searchResult.getKey());
			Assert.assertEquals(toUpdateOriginal.getUrlPosition(), searchResult.getUrlPosition());
			Assert.assertEquals(toUpdateOriginal.getParentCount() + 1, searchResult.getParentCount());
			Assert.assertEquals(true, toUpdateOriginal.getTimestamp() <= searchResult.getTimestamp());
		}
	}
	
	private DummyKVStorable[] readInFile(String filename) throws IOException, FileLockException {
	    List<DummyKVStorable> retrievedLinkData = new ArrayList<DummyKVStorable>();
	    HeaderIndexFile<DummyKVStorable> file = new HeaderIndexFile<DummyKVStorable>(filename, AccessMode.READ_WRITE, 100, new DummyKVStorable().byteBufferSize);
	    long offset = 0;
	    while(offset < file.getFilledUpFromContentStart()) {
	        ByteBuffer linkBuffer = ByteBuffer.allocate(new DummyKVStorable().byteBufferSize);
	        file.read(offset, linkBuffer);
//	        linkBuffer.flip();
	        
	        DummyKVStorable newLinkData = new DummyKVStorable(linkBuffer);
	        retrievedLinkData.add(newLinkData);
	        
	        offset += new DummyKVStorable().byteBufferSize;
	    }
	    file.close();
	    
	    DummyKVStorable[] result = new DummyKVStorable[retrievedLinkData.size()];
	    result = retrievedLinkData.toArray(result);
	    return result;
	}
}