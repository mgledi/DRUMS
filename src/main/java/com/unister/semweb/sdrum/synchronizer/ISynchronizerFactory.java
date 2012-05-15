package com.unister.semweb.sdrum.synchronizer;

import java.io.IOException;

import com.unister.semweb.sdrum.bucket.Bucket;
import com.unister.semweb.sdrum.storable.AbstractKVStorable;

/**
 * The interface for different {@link ISynchronizerFactory}s. Instantiates {@link Synchronizer}s, which synchronize a
 * {@link Bucket} in some way.
 * 
 * @author m.gleditzsch
 */
public interface ISynchronizerFactory<Data extends AbstractKVStorable>  {
    /**
     * Creates a {@link Synchronizer}, which synchronizes the given {@link Bucket}.
     * 
     * @param databaseFilename
     *            the name of the database file which the synchronizer uses
     * @param String
     *            typeOfStorable the classname of the {@link AbstractKVStorable} to be handled by a Synchronizer
     * @return a new {@link Synchronizer}-instance
     * @throws IOException
     *             if some FileHandling fail
     */
    Synchronizer<Data> createSynchronizer(String databaseFilename) throws IOException;
}
