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

package com.gigaspaces.internal.collections;

import java.io.Serializable;

/**
 * @author Niv Ingberg
 * @since 12.0
 */
public interface IntegerObjectMap<V> extends Serializable {

    boolean isEmpty();

    int size();

    V get(int key);

    void put(int key, V value);

    void remove(int key);

    void clear();

    int[] keys();

    V[] getValues(V[] array);

    void flush(IntegerObjectMap<V> target);
}
