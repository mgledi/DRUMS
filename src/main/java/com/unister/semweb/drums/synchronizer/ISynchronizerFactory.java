package com.unister.semweb.drums.synchronizer;

import java.io.IOException;

import com.unister.semweb.drums.GlobalParameters;
import com.unister.semweb.drums.bucket.Bucket;
import com.unister.semweb.drums.storable.AbstractKVStorable;

/**
 * The interface for different {@link ISynchronizerFactory}s. Instantiates {@link Synchronizer}s, which synchronize a
 * {@link Bucket} in some way.
 * 
 * @author m.gleditzsch
 */
public interface ISynchronizerFactory<Data extends AbstractKVStorable> {
    /**
     * Creates a {@link Synchronizer}, which synchronizes the given {@link Bucket}.
     * 
     * @param databaseFilename
     *            the name of the database file which the synchronizer uses
     * @param String
     *            typeOfStorable the classname of the {@link AbstractKVStorable} to be handled by a Synchronizer
     * @param gp
     *            a pointer to the GlobalParameters
     * @return a new {@link Synchronizer}-instance
     * @throws IOException
     *             if some FileHandling fail
     */
    Synchronizer<Data> createSynchronizer(String databaseFilename, GlobalParameters<Data> gp) throws IOException;

}
