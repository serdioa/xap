/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gigaspaces.client.storage_adapters.class_storage_adapters;

import com.gigaspaces.api.ExperimentalApi;
import com.gigaspaces.internal.io.GSByteArrayInputStream;
import com.gigaspaces.internal.io.GSByteArrayOutputStream;

import com.gigaspaces.internal.metadata.PropertyInfo;
import com.gigaspaces.internal.metadata.TypeDesc;
import com.gigaspaces.metadata.SpaceTypeDescriptor;

import java.io.*;
import java.util.Map;

@ExperimentalApi
public class DirectClassBinaryStorageAdapter extends ClassBinaryStorageAdapter {

    private static final int HEADER_BYTES = 1;
    private static final int POSITION_BYTES = 2; // short == 2 bytes

    private static final int MAX_LENGTH = Short.MAX_VALUE * 2; // == -2 when cast to short. -1 is reserved for other uses.

    private static final byte VERSION = 1;

    @Override
    public byte[] toBinary(SpaceTypeDescriptor typeDescriptor, Object[] fields) throws IOException {
        try (GSByteArrayOutputStream bos = new GSByteArrayOutputStream();GSObjectOutputStream out = new GSObjectOutputStream(bos)) {
            out.writeByte(VERSION);

            int numOfFields = fields.length;
            // Write positions placeholders
            for (int i = 0; i < numOfFields; ++i) {
                out.writeShort(-1);
            }

            for (int i = 0; i < numOfFields; ++i) {
                PropertyInfo propertyInfo = ((TypeDesc)typeDescriptor).getSerializedProperties()[i];
                if (hasValue(propertyInfo, fields[i])) {
                    int count = bos.getCount();
                    if (count > MAX_LENGTH) {
                        throw new IOException("Position " + count + " exceeds maximum length " + MAX_LENGTH + " [type=" + typeDescriptor.getTypeName() + ", property=" + propertyInfo.getName() + "]");
                    }
                    bos.writeShort((short) count, i * POSITION_BYTES + HEADER_BYTES);
                    serialize(out, propertyInfo, fields[i]);
                }
            }

            return bos.toByteArray();
        }
    }

    @Override
    public Object[] fromBinary(SpaceTypeDescriptor typeDescriptor, byte[] serializedFields) throws IOException, ClassNotFoundException {
        try (GSByteArrayInputStream bis = new GSByteArrayInputStream(serializedFields); GSObjectInputStream in = new GSObjectInputStream(bis)) {
            assertVersion(in);
            int length = ((TypeDesc)typeDescriptor).getSerializedProperties().length;
            Object[] objects = new Object[length];
            for (int i = 0; i < length; ++i)
                objects[i] = getFieldAtIndex(typeDescriptor, bis, in, i);
            return objects;
        }
    }

    @Override
    public Object getFieldAtIndex(SpaceTypeDescriptor typeDescriptor, byte[] serializedFields, int index) throws IOException, ClassNotFoundException {
        try (GSByteArrayInputStream bis = new GSByteArrayInputStream(serializedFields);GSObjectInputStream in = new GSObjectInputStream(bis)) {
            assertVersion(in);
            return getFieldAtIndex(typeDescriptor, bis, in, index);
        }
    }


    @Override
    public Object[] getFieldsAtIndexes(SpaceTypeDescriptor typeDescriptor, byte[] serializedFields, int... indexes) throws IOException, ClassNotFoundException {
        try (GSByteArrayInputStream bis = new GSByteArrayInputStream(serializedFields); GSObjectInputStream in = new GSObjectInputStream(bis)) {
            assertVersion(in);
            int length = indexes.length;
            Object[] objects = new Object[length];
            for (int i = 0; i < length; ++i)
                objects[i] = getFieldAtIndex(typeDescriptor, bis, in, indexes[i]);
            return objects;
        }
    }

    protected Object getFieldAtIndex(SpaceTypeDescriptor typeDescriptor, GSByteArrayInputStream bis, GSObjectInputStream in , int index)
            throws IOException, ClassNotFoundException {
        bis.setPosition(index * POSITION_BYTES + HEADER_BYTES);
        int position = in.readShort();
        if (position < -1)
            position = position & 0xffff; //https://stackoverflow.com/questions/3153787/how-do-i-print-a-short-as-an-unsigned-short-in-java

        PropertyInfo propertyInfo = ((TypeDesc)typeDescriptor).getSerializedProperties()[index];
        if (position == -1) {
            return getDefaultValue(propertyInfo);
        } else {
            bis.setPosition(position);
            return deserialize(in, propertyInfo);
        }
    }

    @Override
    public byte[] modifyField(SpaceTypeDescriptor typeDescriptor, byte[] serializedFields, int index, Object newValue) throws IOException, ClassNotFoundException {
        Object[] objects = fromBinary(typeDescriptor, serializedFields);
        objects[index] = newValue;
        return toBinary(typeDescriptor, objects);
    }

    @Override
    public byte[] modifyFields(SpaceTypeDescriptor typeDescriptor, byte[] serializedFields, Map<Integer, Object> newValues) throws IOException, ClassNotFoundException {
        Object[] objects = fromBinary(typeDescriptor, serializedFields);
        for (Map.Entry<Integer,Object> entry : newValues.entrySet()){
            objects[entry.getKey()] = entry.getValue();
        }
        return toBinary(typeDescriptor, objects);
    }

    @Override
    public boolean isDirectFieldAccessOptimized() {
        return true;
    }

    private void assertVersion(GSObjectInputStream in) throws IOException {
        byte version = in.readByte();
        if (version != VERSION)
            throw new IllegalStateException("Unsupported version: " + version);
    }
}
