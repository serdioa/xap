package com.gigaspaces.attribute_store;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author Sagiv Michael
 * @since 16.3
 */
public interface SharedReentrantReadWriteLockProvider {
    SharedReentrantReadWriteLock acquireReadLock(String key, long timeout, TimeUnit timeunit) throws IOException, TimeoutException, InterruptedException;
    SharedReentrantReadWriteLock acquireWriteLock(String key, long timeout, TimeUnit timeunit) throws IOException, TimeoutException, InterruptedException;
}
