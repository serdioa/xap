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

package com.gigaspaces.internal.utils.concurrent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;

import java.lang.reflect.Field;

/**
 * @author kimchy
 */
@com.gigaspaces.api.InternalApi
public class UnsafeHolder {

    private static final Logger logger = LoggerFactory.getLogger("com.gigaspaces.core.unsafe");

    private static final Unsafe _unsafe = initUnsafe();

    public static final long BYTE_ARR_OFF = _unsafe != null ? _unsafe.arrayBaseOffset(byte[].class) : 0;

    private static Unsafe initUnsafe() {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            return (Unsafe) field.get(null);
        } catch (Throwable e) {
            logger.debug("Fail to initialize Unsafe.", e);
            return null;
        }
    }

    public static boolean isAvailable() {
        return _unsafe != null;
    }

    public static long objectFieldOffset(Field field) {
        return _unsafe.objectFieldOffset(field);
    }

    public static long allocateMemory(long size) {
        return _unsafe.allocateMemory(size);
    }

    public static long reallocateMemory(long oldAddress, long size) {
        return _unsafe.reallocateMemory(oldAddress, size);
    }

    public static void copyByteArrayToMemory(byte[] data, long address, int length) {
        _unsafe.copyMemory(data, BYTE_ARR_OFF, null, address, length);
    }

    public static void copyByteArrayFromMemory(byte[] destination, long address, int length) {
        _unsafe.copyMemory(null, address, destination, BYTE_ARR_OFF, length);
    }

    public static void freeFromMemory(long address) {
        _unsafe.freeMemory(address);
    }

    public static void putByte(long address, byte b) {
        _unsafe.putByte(address, b);
    }

    public static int getByte(long address) {
        return _unsafe.getByte(address);
    }
}
