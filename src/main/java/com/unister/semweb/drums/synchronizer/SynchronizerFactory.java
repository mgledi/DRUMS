package com.unister.semweb.drums.synchronizer;

import java.io.IOException;

import com.unister.semweb.drums.GlobalParameters;
import com.unister.semweb.drums.storable.AbstractKVStorable;

/**
 * The simplest implementation of a {@link ISynchronizerFactory}.
 * 
 * @author m.gleditzsch
 */
public class SynchronizerFactory<Data extends AbstractKVStorable> implements ISynchronizerFactory<Data> {
    @Override
    public Synchronizer<Data> createSynchronizer(String databaseFilename, GlobalParameters<Data> gp) throws IOException {
        Synchronizer<Data> synchronizer = null;
        synchronizer = new Synchronizer<Data>(databaseFilename, gp);
        return synchronizer;
    }
}
