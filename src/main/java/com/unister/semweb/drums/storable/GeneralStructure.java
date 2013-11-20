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
package com.unister.semweb.drums.storable;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class represents the structure of one {@link GeneralStorable}-Type. It is comparable with a "Create Table" in
 * other databases.
 * 
 * @author Martin Nettling
 */
public class GeneralStructure implements Serializable {
    /** UID for serializing */
    private static final long serialVersionUID = -7622872763812113528L;
    /** the logger for this class */
    static Logger logger = LoggerFactory.getLogger(GeneralStructure.class);

    /** All allowed basic types. */
    public static enum Basic_Field_Types {
        Byte(1), Boolean(1), Char(2), Short(2), ShortInt(2), Integer(4), Float(4), Double(8), Long(8);

        public int size;

        Basic_Field_Types(int byteSize) {
            this.size = byteSize;
        }
    }

    boolean INSTANCE_EXISITS = false;

    ArrayList<String> keyPartNames = new ArrayList<String>();
    ArrayList<String> valuePartNames = new ArrayList<String>();

    HashMap<Integer, Integer> keyHash2Index = new HashMap<Integer, Integer>();
    HashMap<Integer, Integer> valueHash2Index = new HashMap<Integer, Integer>();

    ArrayList<Integer> keyIndex2Hash = new ArrayList<Integer>();
    ArrayList<Integer> valueIndex2Hash = new ArrayList<Integer>();

    ArrayList<Integer> keySizes = new ArrayList<Integer>();
    ArrayList<Integer> valueSizes = new ArrayList<Integer>();

    ArrayList<Integer> keyByteOffsets = new ArrayList<Integer>();
    ArrayList<Integer> valueByteOffsets = new ArrayList<Integer>();

    int keySize = 0;
    int valueSize = 0;
    
    /**
     * Adds a new ValuePart
     * 
     * @param name
     *            the name of the key part. With this name you can access this part
     * @param size
     *            the size of the key part in bytes
     * @return true if adding the value part was successful
     * @throws IOException 
     */
    public boolean addValuePart(String name, int size) throws IOException {
        if (INSTANCE_EXISITS) {
            throw new IOException("A GeneralStroable was already instantiated. You cant further add Value Parts");
        }
        int hash = Arrays.hashCode(name.getBytes());
        int index = valuePartNames.size();
        if (valueHash2Index.containsKey(hash)) {
            logger.error("A valuePart with the name {} already exists", name);
            return false;
        }
        valuePartNames.add(name);
        valueHash2Index.put(hash, index);
        valueIndex2Hash.add(hash);
        valueSizes.add(size);
        valueByteOffsets.add(valueSize);
        valueSize += size;
        return true;
    }

    /**
     * Adds a new ValuePart. This is a wrapper method for <code>addKeyPart(String, int)</code>.
     * 
     * @param name
     *            the name of the key part. With this name you can access this part
     * @param type
     *            the type of the key part.
     * @return true if adding the value part was successful
     * @throws IOException 
     */
    public boolean addValuePart(String name, Basic_Field_Types type) throws IOException {
        return addValuePart(name, type.size);
    }

    /**
     * Adds a new KeyPart
     * 
     * @param name
     *            the name of the key part. With this name you can access this part
     * @param size
     *            the size of the key part in bytes
     * @return true if adding the key part was successful
     * @throws IOException 
     */
    public boolean addKeyPart(String name, int size) throws IOException {
        if (INSTANCE_EXISITS) {
            throw new IOException("A GeneralStroable was already instantiated. You cant further add Key Parts");
        }
        int hash = Arrays.hashCode(name.getBytes());
        int index = keyPartNames.size();
        if (keyHash2Index.containsKey(hash)) {
            logger.error("A keyPart with the name {} already exists", name);
            return false;
        }
        keyPartNames.add(name);
        keyHash2Index.put(hash, index);
        keyIndex2Hash.add(hash);
        keySizes.add(size);
        keyByteOffsets.add(keySize);
        keySize += size;
        return true;
    }

    /**
     * Adds a new KeyPart. This is a wrapper method for <code>addKeyPart(String, int)</code>.
     * 
     * @param name
     *            the name of the key part. With this name you can access this part
     * @param type
     *            the type of the key part.
     * @return true if adding the key part was successful
     * @throws IOException 
     */
    public boolean addKeyPart(String name, Basic_Field_Types type) throws IOException {
        return addKeyPart(name, type.size);
    }
}
