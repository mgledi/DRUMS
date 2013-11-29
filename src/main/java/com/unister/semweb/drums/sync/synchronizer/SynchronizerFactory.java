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
package com.unister.semweb.drums.sync.synchronizer;

import java.io.IOException;

import com.unister.semweb.drums.GlobalParameters;
import com.unister.semweb.drums.storable.AbstractKVStorable;
import com.unister.semweb.drums.storable.GeneralStorable;

/**
 * The simplest implementation of a {@link ISynchronizerFactory}.
 * 
 * @author Martin Nettling
 * @param <Data>
 *            an implementation of {@link AbstractKVStorable}, e.g. {@link GeneralStorable}
 */
public class SynchronizerFactory<Data extends AbstractKVStorable> implements ISynchronizerFactory<Data> {
    @Override
    public Synchronizer<Data> createSynchronizer(String databaseFilename, GlobalParameters<Data> gp) throws IOException {
        Synchronizer<Data> synchronizer = null;
        synchronizer = new Synchronizer<Data>(databaseFilename, gp);
        return synchronizer;
    }
}
