package com.unister.semweb.superfast.storage.synchronizer;

import java.io.IOException;

import com.unister.semweb.superfast.storage.storable.AbstractKVStorable;

/**
 * The simplest implementation of a {@link ISynchronizerFactory}.
 * 
 * @author m.gleditzsch
 */
public class SynchronizerFactory<Data extends AbstractKVStorable<Data>> implements ISynchronizerFactory<Data> {

    /** a prototype from a concrete AbstractKVStorable */
    final private Data prototype;

    public SynchronizerFactory(Data prototype) {
        this.prototype = prototype;
    }

    @Override
    public Synchronizer<Data> createSynchronizer(String databaseFilename) throws IOException {
        Synchronizer<Data> synchronizer = null;
        try {
            synchronizer = new Synchronizer<Data>(databaseFilename, prototype.clone());
        } catch (CloneNotSupportedException e) {
            throw new IOException("TODO"); // TODO
        }
        return synchronizer;
    }
}
