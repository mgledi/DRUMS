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
package com.unister.semweb.drums.bucket;

/**
 * This exception is thrown, if the a {@link Bucket} is tried to access, which doesn't exist or an element could not be
 * added eitherway.
 * 
 * @author Martin Nettling
 * 
 */
public class BucketContainerException extends Exception {
    private static final long serialVersionUID = -7034481737223723755L;

    /**
     * Instantiates a new BucketContainerException with the given message
     * 
     * @param message
     */
    public BucketContainerException(String message) {
        super(message);
    }
}
