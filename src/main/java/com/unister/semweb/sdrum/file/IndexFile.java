package com.unister.semweb.sdrum.file;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.unister.semweb.sdrum.GlobalParameters;
import com.unister.semweb.sdrum.bucket.hashfunction.AbstractHashFunction;
import com.unister.semweb.sdrum.storable.AbstractKVStorable;

/**
 * This class represents the index used by SDRUM. It contains for each {@link HeaderIndexFile} its
 * {@link IndexForHeaderIndexFile}.
 * 
 * @author m.gleditzsch
 */
public class IndexFile<Data extends AbstractKVStorable> extends AbstractHeaderFile {
    protected static Logger logger = LoggerFactory.getLogger(IndexFile.class);

    /** A pointer to the GlobalParameters used by this SDRUM */
    protected GlobalParameters<Data> gp;

    /**
     * A map with all indexes. The key equals the fileoffset in the {@link IndexFile}, where the belonging index is
     * written.
     */
    public LongObjectOpenHashMap<IndexForHeaderIndexFile<Data>> indices;

    private byte closedSoftly;

    /**
     * @param gp
     * @throws FileLockException
     * @throws IOException
     */
    public IndexFile(GlobalParameters<Data> gp) throws FileLockException,
            IOException {
        this.gp = gp;
        String filename = gp.databaseDirectory + "/" + gp.INDEX_FILE_NAME;
        logger.info("Opening index {}", filename);
        this.indices = new LongObjectOpenHashMap<IndexForHeaderIndexFile<Data>>();
        this.osFile = new File(filename);
        this.init();
    }

    /** Creates the file and / or opens the FileChannel to this file */
    protected void init() throws FileLockException, IOException {
        this.contentStart = HEADER_SIZE;
        if (!osFile.exists()) {
            logger.debug("File {} not found. Initialise new File.", osFile.getAbsolutePath());
            this.createFile();
        } else {
            logger.debug("File {} exists. Try to open it.", osFile.getAbsolutePath());
            openChannel();
        }
        this.contentEnd = size;
    }

    /**
     * Loads all possible indices for each HeaderIndexFile from the index file.
     * 
     * @param hashFunction
     * @throws FileLockException
     * @throws IOException
     */
    public void loadIndices(final AbstractHashFunction hashFunction) throws FileLockException, IOException {
        for (int bid = 0; bid < hashFunction.getNumberOfBuckets(); bid++) {
            String fileName = gp.databaseDirectory + "/" + hashFunction.getFilename(bid);
            if (new File(fileName).exists()) {
                logger.info("Loading index for {}", fileName);
                HeaderIndexFile<Data> file = new HeaderIndexFile<Data>(fileName, 1, gp);
                this.getIndexByFileOffset(file);
                file.close();
            }
        }
    }

    @Override
    public void close() {
        this.indices = null; // TODO: close correctly the mappedbytebuffers
        super.close();
    }

    public long addNewIndex() {
        logger.info("Creating space for new Index");
        long indexPos = filledUpTo;
        if (filledUpTo + IndexForHeaderIndexFile.INDEX_SIZE < contentEnd) {
            size += IndexForHeaderIndexFile.INDEX_SIZE * 16;
            logger.info("Enlarge filesize of {} to {}", osFile, size);
        }
        filledUpTo += IndexForHeaderIndexFile.INDEX_SIZE;
        return indexPos;
    }

    public void generateIndicesFromFiles() {

    }

    @Override
    protected void readHeader() {
        headerBuffer.rewind();
        size = headerBuffer.getLong();
        filledUpTo = headerBuffer.getLong();
        closedSoftly = headerBuffer.get();
    }

    @Override
    protected void writeHeader() {
        headerBuffer.rewind();
        headerBuffer.putLong(size);
        headerBuffer.putLong(filledUpTo);
        headerBuffer.put(closedSoftly);
    }

    /** true, if the file was closed softly */
    public boolean isSoftlyClosed() {
        return closedSoftly == 1;
    }

    protected void setSoftlyClosed(boolean value) {
        if (headerBuffer == null) {
            return;
        }
        closedSoftly = (byte) (value ? 1 : 0);
        this.writeHeader();
    }

    public IndexForHeaderIndexFile<Data> getIndexByFileOffset(HeaderIndexFile<Data> file) throws IOException {
        long offset = file.indexFileOffset;
        if (!indices.containsKey(offset)) {
            MappedByteBuffer buffer = channel.map(MapMode.READ_WRITE, offset, IndexForHeaderIndexFile.INDEX_SIZE);
            IndexForHeaderIndexFile<Data> index = new IndexForHeaderIndexFile<Data>(gp, buffer);
            indices.put(offset, index);
            if (index.checkSum != file.checkSum) {
                logger.warn(
                        "Checksum in index differs from checksum in file {}. Index have to been rebuild from file.",
                        file.osFile);
            }

        }
        return indices.get(offset);
    }
    /**
     * TODO:
     * is index always large enough
     */
}
