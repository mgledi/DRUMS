/* Copyright (C) 2012-2013 Unister GmbH
 * 
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA. */
package com.unister.semweb.drums.api;

/**
 * Is thrown when an error occurs within the file storage.
 * 
 * @author Nils Thieme
 * 
 */
public class DRUMSException extends Exception {
    private static final long serialVersionUID = -6381038599235245823L;

    public DRUMSException() {
        super();
    }

    public DRUMSException(String message) {
        super(message);
    }

    public DRUMSException(String message, Throwable throwable) {
        super(message, throwable);
    }

    public DRUMSException(Throwable throwable) {
        super(throwable);
    }
}
