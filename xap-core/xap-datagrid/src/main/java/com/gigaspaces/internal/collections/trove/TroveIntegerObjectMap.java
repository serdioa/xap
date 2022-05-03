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

package com.gigaspaces.internal.collections.trove;

import com.gigaspaces.internal.collections.IntegerObjectMap;
import com.gigaspaces.internal.gnu.trove.TIntObjectHashMap;

/**
 * @author Niv Ingberg
 * @since 12.0
 */
@com.gigaspaces.api.InternalApi
public class TroveIntegerObjectMap<V> implements IntegerObjectMap<V> {
    private static final long serialVersionUID = -8321502498200244722L;
    private final TIntObjectHashMap<V> map = new TIntObjectHashMap<>();

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public V get(int key) {
        return map.get(key);
    }

    @Override
    public void put(int key, V value) {
        map.put(key, value);
    }

    @Override
    public void remove(int key) {
        map.remove(key);
    }

    @Override
    public void clear() {
        map.clear();
    }

    @Override
    public int[] keys() {
        return map.keys();
    }

    @Override
    public V[] getValues(V[] array) {
        return map.getValues(array);
    }

    @Override
    public void flush(IntegerObjectMap<V> target) {
        map.forEachEntry((key, value) -> {
            target.put(key, value);
            return true;
        });
    }
}
