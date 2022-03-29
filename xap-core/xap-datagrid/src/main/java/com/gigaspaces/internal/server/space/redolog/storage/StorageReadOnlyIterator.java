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

package com.gigaspaces.internal.server.space.redolog.storage;

import com.gigaspaces.internal.utils.collections.ReadOnlyIterator;

import java.util.NoSuchElementException;

/**
 * An iterator which support read only operation over a {@link IRedoLogFileStorage}
 *
 * @author eitany
 * @since 7.1
 */
public interface StorageReadOnlyIterator<E> extends ReadOnlyIterator<E> {
    /**
     * Returns <tt>true</tt> if the iteration has more elements. (In other words, returns
     * <tt>true</tt> if <tt>next</tt> would return an element rather than throwing an exception.)
     *
     * @return <tt>true</tt> if the iterator has more elements.
     */
    boolean hasNext() throws StorageException;

    /**
     * Returns the next element in the iteration.  Calling this method repeatedly until the {@link
     * #hasNext()} method returns false will return each element in the underlying collection
     * exactly once.
     *
     * @return the next element in the iteration.
     * @throws NoSuchElementException iteration has no more elements.
     */
    E next() throws StorageException;

    /**
     * Closes the iterator
     */
    void close() throws StorageException;
}
