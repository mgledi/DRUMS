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
package com.unister.semweb.drums.synchronizer;

import java.io.IOException;

import com.unister.semweb.drums.GlobalParameters;
import com.unister.semweb.drums.bucket.Bucket;
import com.unister.semweb.drums.storable.AbstractKVStorable;

/**
 * The interface for different {@link ISynchronizerFactory}s. Instantiates {@link Synchronizer}s, which synchronize a
 * {@link Bucket} in some way.
 * 
 * @author Martin Gleditzsch
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
