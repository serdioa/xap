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
