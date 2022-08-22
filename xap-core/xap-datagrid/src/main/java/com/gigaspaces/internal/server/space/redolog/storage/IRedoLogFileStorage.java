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

import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.server.space.redolog.RedoLogFileCompromisedException;
import com.gigaspaces.internal.server.space.redolog.storage.bytebuffer.WeightedBatch;
import com.j_spaces.core.cluster.startup.CompactionResult;

import java.util.List;

/**
 * Provide an external storage for a redo log file, Packets are stored as a single list, adding
 * packets are always appended at the end of the list, and removal of packets are always from the
 * start of the list
 *
 * Implementor should support concurrent readers or a single writer, in other words, the implementor
 * can assume access to this structure are guarded with a reader writer lock according to the
 * operation type
 *
 * An exception is {@link #size()} method which should not assume a reader lock is held.
 *
 * @author eitany
 * @since 7.1
 */
public interface IRedoLogFileStorage<T extends IReplicationOrderedPacket> extends IRedoLogFileStorageStatistics {
    /**
     * Adds a batch of packets that will be stored at the end of the list
     *
     * @param replicationPackets packets to store
     */
    void appendBatch(List<T> replicationPackets) throws StorageException, StorageFullException;

    /**
     * This method should not assume a reader lock is obtained when accessing it
     *
     * @return number of packets in the storage
     */
    long size() throws StorageException;

    /**
     * Removes a batch from the start of the list
     *
     * @param batchCapacity WeightToRemove to remove
     * @param lastCompactionRangeEndKey packets with keys larger than this cannot be discarded
     * @return removed batch
     */
    WeightedBatch<T> removeFirstBatch(int batchCapacity, long lastCompactionRangeEndKey) throws StorageException;

    /**
     * Delete a batch from the start of the list
     *
     * @param packetsCount number of packets to delete
     */
    void deleteOldestPackets(long packetsCount) throws StorageException;

    /**
     * @param fromIndex index to start iterating from
     * @return read only iterator that starts from the specified index
     */
    StorageReadOnlyIterator<T> readOnlyIterator(long fromIndex) throws StorageException;

    /**
     * @return true if the storage has no packets
     */
    boolean isEmpty() throws StorageException;

    /**
     * Validates the integrity of the storage
     */
    void validateIntegrity() throws RedoLogFileCompromisedException;

    /**
     * Close the storage and clears its resources, the storage can no longer be used.
     */
    void close();

    long getWeight();

    long getDiscardedPacketsCount();

    CompactionResult performCompaction(long from, long to);

    long getCacheWeight();

    default T getOldest() throws StorageException {
        throw new StorageException("Unsupported");
    }

    default T removeOldest() throws StorageException {
        throw new StorageException("Unsupported");
    }

    default long getOldestKey() throws StorageException {
        throw new StorageException("Unsupported");
    }
}
