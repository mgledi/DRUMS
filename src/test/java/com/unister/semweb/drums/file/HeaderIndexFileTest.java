/*
 * Copyright (C) 2012-2013 Unister GmbH
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */
package com.unister.semweb.drums.file;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Arrays;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.unister.semweb.drums.GlobalParameters;
import com.unister.semweb.drums.file.AbstractHeaderFile.AccessMode;
import com.unister.semweb.drums.storable.DummyKVStorable;

/**
 * tests the Handling with the DB-File
 * 
 * @author Martin Nettling
 */
public class HeaderIndexFileTest {
    HeaderIndexFile<DummyKVStorable> file;

    /** The global Parameters to use */
    GlobalParameters<DummyKVStorable> gp = new GlobalParameters<DummyKVStorable>(DummyKVStorable.getInstance());
    
    /** create static file access */
    @BeforeClass
    public static void deleteInitialFile() throws IOException, FileLockException {
        if (new File("test.db").exists()) {
            new File("test.db").delete();
        }
    }

    /** create static file access */
    @Before
    public void createFile() throws IOException, FileLockException {
        file = new HeaderIndexFile<DummyKVStorable>("test.db", HeaderIndexFile.AccessMode.READ_WRITE, 1, gp);
    }

    /** create static file access */
    @After
    public void closeAndDeleteFile() throws IOException, FileLockException {
        if (file != null && file.osFile.exists()) {
            file.delete();
        }
        file = null;
    }

    @Test
    /** checks if the file exists */
    public void fileExists() throws IOException, FileLockException {
        System.out.println("======== File Exists test");
        Assert.assertTrue(file.osFile.exists());
    }

    @Test
    /** checks if the calculated free space is correct */
    public void getFreeSpaceTest() throws IOException {
        System.out.println("======== getFreeSpaceTest");
        long freeSpace = file.getFreeSpace();
        long realSpace = file.size - HeaderIndexFile.HEADER_SIZE - HeaderIndexFile.MAX_INDEX_SIZE_IN_BYTES;
        Assert.assertEquals(freeSpace, realSpace);
    }

    @Test
    /** checks if closing and reopening is possible and if the (empty) Header was stored correctly */
    public void closeReopenTest() throws IOException, FileLockException {
        System.out.println("======== closeReopenTest");
        byte[] oldHeader = new byte[1024];
        file.headerBuffer.rewind();
        file.headerBuffer.get(oldHeader);
        file.close();

        createFile();
        byte[] newHeader = new byte[1024];
        file.headerBuffer.rewind();
        file.headerBuffer.get(newHeader);

        Assert.assertTrue(Arrays.equals(oldHeader, newHeader));
    }

    @Test
    /** checks if the calculated free space is correct */
    public void fileWrite() throws IOException {
        System.out.println("======== fileWrite");
        byte[] b = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 };
        file.write(0, ByteBuffer.wrap(b));
        Assert.assertEquals(file.filledUpTo, HeaderIndexFile.HEADER_SIZE + HeaderIndexFile.MAX_INDEX_SIZE_IN_BYTES + 16);
    }

    @Test
    /** checks if the calculated free space is correct */
    public void fileRead() throws IOException {
        System.out.println("======== fileRead");
        byte[] b = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 };
        file.write(0, ByteBuffer.wrap(b));

        byte[] r = new byte[16];
        file.read(0, ByteBuffer.wrap(r));
        Assert.assertTrue(Arrays.equals(b, r));
    }

    @Test
    /** checks if the calculated free space is correct */
    public void mapWrite() throws IOException {
        System.out.println("======== mapWrite");
        byte[] b = { 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1 };
        byte[] r = new byte[16];
        MappedByteBuffer mbb = file.map(0, 16);
        // mbb.rewind();
        mbb.put(b);
        mbb.force();
        mbb.rewind();
        mbb.get(r);
        Assert.assertTrue(Arrays.equals(b, r));
    }

    @Test
    /** checks if the calculated free space is correct */
    public void mapRead() throws IOException, FileLockException {
        System.out.println("======== mapRead");
        byte[] b = { 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1 };
        byte[] r = new byte[16];

        // --------------- write to file
        MappedByteBuffer mbb = file.map(0, 16);
        // mbb.rewind();
        mbb.put(b);
        mbb.force();
        mbb.rewind();
        file.close();

        // --------------- read from file
        this.createFile();

        mbb = file.map(0, 16);
        mbb.get(r);
        file.read(0, ByteBuffer.wrap(r));
        Assert.assertTrue(Arrays.equals(b, r));
    }

    @Test
    /** test if the file is locked, and no other HeaderIndexFile can be instantiated on this OSFile. Also tests retries */
    public void lockTest() throws IOException, FileLockException {
        System.out.println("======== lockTest");
        file.close();
        createFile();
        try {
            new HeaderIndexFile<DummyKVStorable>("test.db", HeaderIndexFile.AccessMode.READ_WRITE, 2, gp);
            Assert.assertTrue(false);
        } catch (FileLockException e) {
            Assert.assertTrue(true);
        }
        file.close();

        new HeaderIndexFile<DummyKVStorable>("test.db", HeaderIndexFile.AccessMode.READ_WRITE, 2, gp);
        Assert.assertTrue(true);
    }

    @Test
    public void indexTest() throws IOException, FileLockException {
        System.out.println("======== indexTest");
        file.delete();
        
        createFile();
        file.close();

        file.openChannel();
        byte[] dst = new byte[file.elementSize];
        long oldOffset = 0;
        for(long i=0; i < 100000; i ++) {
            ByteBuffer b = ByteBuffer.wrap(dst);
            b.putLong(i);
            file.append(b);
            int idx = file.getChunkIndex(oldOffset);
//            file.index.setLargestKey(idx, i);
            oldOffset += dst.length;
        }
        file.close();
    }

    @Test
    public void deleteFile() throws Exception {
        System.out.println("======== deleteTest");
        file.delete();
        Assert.assertTrue(!(new File("test.db").exists()));
    }

    @Test
    public void test() throws Throwable {
        HeaderIndexFile<DummyKVStorable> hif = new HeaderIndexFile<DummyKVStorable>("/tmp/test.db", AccessMode.READ_WRITE, 1, gp);
        hif.delete();

        File osFile = new File("/tmp/test.db");
        osFile.createNewFile();
        RandomAccessFile raf = new RandomAccessFile(osFile, "rw");
        FileChannel channel = raf.getChannel();
        MappedByteBuffer bb = channel.map(MapMode.READ_WRITE, 0, 4);
        bb = null;
        channel.write(ByteBuffer.allocate(10).putLong(3));
        raf.close();

        System.gc();
        System.out.println(osFile.delete());
    }
}
