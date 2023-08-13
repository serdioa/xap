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
