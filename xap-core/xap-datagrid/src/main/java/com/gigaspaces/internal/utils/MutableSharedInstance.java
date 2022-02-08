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
package com.gigaspaces.internal.utils;

import java.util.concurrent.atomic.AtomicInteger;

@com.gigaspaces.api.InternalApi
public class MutableSharedInstance<T> {

    private T _instance;
    private final AtomicInteger _refCount;

    public MutableSharedInstance(T instance) {
        this._instance = instance;
        this._refCount = new AtomicInteger(0);
    }

    public synchronized void updateInstance(T _instance) {
        this._instance = _instance;
    }

    public T value() {
        return _instance;
    }

    public int count() {
        return _refCount.get();
    }

    public int increment() {
        return _refCount.incrementAndGet();
    }

    public int decrement() {
        return _refCount.decrementAndGet();
    }
}
