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

/**
 * This class extends the {@link Exception}-class. A {@link FileLockException} should be thrown if a File is locked in
 * some way.
 * 
 * @author Martin Nettling
 * 
 */
public class FileLockException extends Exception {
    private static final long serialVersionUID = 1L;

    /**
     * This method constructs a new {@link FileLockException}
     * 
     * @param String
     *            message, the message to throw
     */
    public FileLockException(String message) {
        super(message);
    }
}
