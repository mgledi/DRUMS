package com.unister.semweb.superfast.storage;

import junit.framework.Assert;

import org.junit.Test;

import com.unister.semweb.superfast.storage.storable.DummyKVStorable;

public class LinkDataMergeTest {

	@Test
	public void mergeRelevanceScoreTest1(){
		DummyKVStorable ld1 = new DummyKVStorable();
		ld1.setRelevanceScore(0.34);
		ld1.setParentCount(1);
		DummyKVStorable ld2 = new DummyKVStorable();
		ld2.setRelevanceScore(0.23);
		ld2.setParentCount(3);
		
		DummyKVStorable ldResult = (DummyKVStorable)ld1.merge(ld2);
		Assert.assertEquals(0.2575, ldResult.getRelevanceScore(), 1/32767d);
		Assert.assertEquals(4, ldResult.getParentCount());
	}
	@Test
	public void mergeRelevanceScoreTest2(){
		DummyKVStorable ld1 = new DummyKVStorable();
		ld1.setRelevanceScore(0.23);
		ld1.setParentCount(1);
		DummyKVStorable ld2 = new DummyKVStorable();
		ld2.setRelevanceScore(-1);
		ld2.setParentCount(1);
		
		DummyKVStorable ldResult = (DummyKVStorable)ld1.merge(ld2);
		Assert.assertEquals(0.23, ldResult.getRelevanceScore(), 1/32767d);
		Assert.assertEquals(2, ldResult.getParentCount());
	}
	@Test
	public void mergeRelevanceScoreTest3(){
		DummyKVStorable ld1 = new DummyKVStorable();
		ld1.setRelevanceScore(-0.23);
		ld1.setParentCount(1);
		DummyKVStorable ld2 = new DummyKVStorable();
		ld2.setRelevanceScore(-1);
		ld2.setParentCount(1);
		
		DummyKVStorable ldResult = (DummyKVStorable)ld1.merge(ld2);
		Assert.assertEquals(0.0, ldResult.getRelevanceScore(), 1/32767d);
		Assert.assertEquals(2, ldResult.getParentCount());
	}
	@Test
	public void mergeRelevanceScoreTest4(){
		DummyKVStorable ld1 = new DummyKVStorable();
		ld1.setRelevanceScore(-0.23);
		ld1.setParentCount(1);
		DummyKVStorable ld2 = new DummyKVStorable();
		ld2.setRelevanceScore(0.1);
		ld2.setParentCount(1);
		
		DummyKVStorable ldResult = (DummyKVStorable)ld1.merge(ld2);
		Assert.assertEquals(0.1, ldResult.getRelevanceScore(), 1/32767d);
		Assert.assertEquals(2, ldResult.getParentCount());
	}
	
	/** Here we test the precision if the parent count is very large.*/
	@Test
    public void mergeRelevanceScoreTest5(){
        DummyKVStorable ld1 = new DummyKVStorable();
        ld1.setRelevanceScore(0.23);
        ld1.setParentCount(2000000);
        DummyKVStorable ld2 = new DummyKVStorable();
        ld2.setRelevanceScore(0.34);
        ld2.setParentCount(1);
        
        DummyKVStorable ldResult = (DummyKVStorable)ld1.merge(ld2);
        Assert.assertEquals(0.2575, ldResult.getRelevanceScore(), 1/32767d * 2000000);
        Assert.assertEquals(2000001, ldResult.getParentCount());
    }
	
	@Test
	public void mergeTimestampTest4(){
		DummyKVStorable ld1 = new DummyKVStorable();
		// We take Christmas at 2011 as test value.
		ld1.setTimestamp(1324681200000L);
		DummyKVStorable ld2 = new DummyKVStorable();
		ld2.setTimestamp(200);
		
		DummyKVStorable ldResult = (DummyKVStorable)ld1.merge(ld2);
		Assert.assertEquals(1324681200000L, ldResult.getTimestamp());
	}
}
