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

package com.gigaspaces.attribute_store;

import com.gigaspaces.internal.io.IOUtils;

import java.io.*;

/**
 * @author yechiel
 * @since 10.2
 */
public interface AttributeStore extends Closeable {

    String get(String key) throws IOException;

    String set(String key, String value) throws IOException;

    byte[] getBytes(String key) throws IOException;

    byte[] setBytes(String key, byte[] value) throws IOException;

    default <T> T getObject(String key) throws IOException {
        byte[] bytes = getBytes(key);
        if (bytes == null || bytes.length == 0)
            return null;
        try {
            return IOUtils.readObject(new ObjectInputStream(new ByteArrayInputStream(bytes)));
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to deserialize InternalQuiesceDetails", e);
        }
    }

    default <T> void setObject(String key, T value) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
        IOUtils.writeObject(objectOutputStream, value);
        byte[] bytes = byteArrayOutputStream.toByteArray();
        setBytes(key, bytes);
    }

    String remove(String key) throws IOException;

    SharedLockProvider getSharedLockProvider();

    SharedReentrantReadWriteLockProvider getSharedReentrantReadWriteLockProvider();
}
