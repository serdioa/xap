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
package com.gigaspaces.internal.utils.collections;

import java.util.concurrent.ConcurrentSkipListMap;

/**
 * ConcurrentSkipListMap doesn't throw a ClassCastException on the first key if it is not Comparable.
 * It'll throw an exception if you add a second key. This is just because ConcurrentSkipListMap doesn't
 * have special logic to verify that the first key is comparable. This is a JDK oversight that might be
 * fixed in the future.
 * <p>
 * After successful insertion of a non-Comparable key, removal is also affected by this, and the key can't
 * be removed.
 *
 * @param <K> key
 * @param <V> value
 * @since 16.4
 */
public class CheckedConcurrentSkipListMap<K, V> extends ConcurrentSkipListMap<K, V> {
    private static final long serialVersionUID = -8283771404301975962L;

    @Override
    public V putIfAbsent(K key, V value) {
        if (!Comparable.class.isAssignableFrom(key.getClass()))
            throw new ClassCastException(key.getClass() + " must implement Comparable");
        return super.putIfAbsent(key, value);
    }

    @Override
    public V put(K key, V value) {
        if (!Comparable.class.isAssignableFrom(key.getClass()))
            throw new ClassCastException(key.getClass() + " must implement Comparable");
        return super.put(key, value);
    }
}
