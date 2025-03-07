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

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author yechiel
 * @since 10.2
 */
@com.gigaspaces.api.InternalApi
public class TransientAttributeStore implements AttributeStore, Serializable {
    private static final long serialVersionUID = -7740891239839069999L;
    private final ConcurrentHashMap<String, String> store = new ConcurrentHashMap<String, String>();

    @Override
    public String get(String name) {
        return store.get(name);
    }

    @Override
    public String set(String name, String value) {
        return store.put(name, value);
    }

    @Override
    public String remove(String key) {
        return store.remove(key);
    }

    @Override
    public SharedLockProvider getSharedLockProvider() {
        throw new UnsupportedOperationException("This attribute store does not support shared locks");
    }

    @Override
    public SharedReentrantReadWriteLockProvider getSharedReentrantReadWriteLockProvider() {
        throw new UnsupportedOperationException("This attribute store does not support shared reentrant read write locks");
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public byte[] getBytes(String key) throws IOException {
        throw new UnsupportedOperationException("only supported in ZooKeeperAttributeStore.class");
    }

    @Override
    public byte[] setBytes(String key, byte[] value) throws IOException {
        throw new UnsupportedOperationException("only supported in ZooKeeperAttributeStore.class");
    }
}
