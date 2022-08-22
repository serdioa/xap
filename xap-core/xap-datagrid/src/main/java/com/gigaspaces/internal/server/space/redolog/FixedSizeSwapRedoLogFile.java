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

package com.gigaspaces.internal.server.space.redolog;

import com.gigaspaces.internal.cluster.node.impl.backlog.AbstractSingleFileGroupBacklog;
import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.server.space.redolog.storage.INonBatchRedoLogFileStorage;
import com.gigaspaces.internal.server.space.redolog.storage.StorageException;
import com.gigaspaces.internal.server.space.redolog.storage.StorageReadOnlyIterator;
import com.gigaspaces.internal.server.space.redolog.storage.bytebuffer.WeightedBatch;
import com.gigaspaces.internal.utils.collections.ReadOnlyIterator;
import com.gigaspaces.logger.Constants;
import com.j_spaces.core.cluster.ReplicationPolicy;
import com.j_spaces.core.cluster.startup.CompactionResult;
import com.j_spaces.core.cluster.startup.RedoLogCompactionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.NoSuchElementException;

/**
 * A swap based implementation of the {@link IRedoLogFile} interface, A fixed number of packets can
 * be held in the memory and once this number is exceeded the other packets are stored in a provided
 * {@link INonBatchRedoLogFileStorage}
 *
 * @author eitany
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class FixedSizeSwapRedoLogFile<T extends IReplicationOrderedPacket> implements IRedoLogFile<T> {
    private static final Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_REPLICATION_BACKLOG);

    private final int _memoryMaxCapacity; //max allowed weight that memory can hold in any time
    private final int _fetchBatchCapacity;
    private final MemoryRedoLogFile<T> _memoryRedoLogFile;
    private final INonBatchRedoLogFileStorage<T> _externalStorage;
    private final String _name;
    private final int _combinedMemoryMaxCapacity;
    private final AbstractSingleFileGroupBacklog _groupBacklog;
    //Not volatile because this is not a thread safe structure, assume flushing of thread cache
    //changes because lock is held at upper layer
    private boolean _insertToExternal = false;
    private long _lastCompactionRangeEndKey = -1;
    private long _lastSeenTransientPacketKey = -1;

    /**
     * Constructs a fixed size swap redo log file
     */
    public FixedSizeSwapRedoLogFile(FixedSizeSwapRedoLogFileConfig config, String name, AbstractSingleFileGroupBacklog groupBacklog) {
        this._memoryMaxCapacity = config.getMemoryMaxPackets();
        this._externalStorage = config.getRedoLogFileStorage();
        this._fetchBatchCapacity = config.getFetchBatchSize();
        this._combinedMemoryMaxCapacity = config.getCombinedMemoryMaxCapacity();

        if (_logger.isDebugEnabled()) {
            _logger.debug("FixedSizeSwapRedoLogFile created:"
                    + "\n\tmemoryMaxPackets = " + _memoryMaxCapacity
                    + "\n\tfetchBatchSize = " + _fetchBatchCapacity);
        }
        _memoryRedoLogFile = new MemoryRedoLogFile<T>(name, groupBacklog);
        _name = name;
        _groupBacklog = groupBacklog;
    }

    @Override
    public void add(T replicationPacket) {
        int packetWeight = replicationPacket.getWeight();
        if (!_insertToExternal) {
            if (_memoryRedoLogFile.isEmpty() && packetWeight > _combinedMemoryMaxCapacity) {
                _memoryRedoLogFile.add(replicationPacket);
                _logger.warn("inserting to " + _name + " memory an operation which weight is larger than the max memory capacity:" +
                        " packet[key=" + replicationPacket.getKey() + ",Type=" + replicationPacket.getClass() + ", weight=" + replicationPacket.getWeight() + "]\n");
                return;
            }
            if (_memoryRedoLogFile.getWeight() + packetWeight <= _memoryMaxCapacity) {
                _memoryRedoLogFile.add(replicationPacket);
            } else {
                _insertToExternal = true;
            }
        }
        if (_insertToExternal)
            addToStorage(replicationPacket);

        if (RedoLogCompactionUtil.isCompactable(replicationPacket)) {
            _lastSeenTransientPacketKey = replicationPacket.getKey();
        }
    }

    @Override
    public T getOldest() {
        if (!_memoryRedoLogFile.isEmpty())
            return _memoryRedoLogFile.getOldest();

        return getOldestFromDataStorage();
    }

    @Override
    public long getOldestKey() {
        if (!_memoryRedoLogFile.isEmpty())
            return _memoryRedoLogFile.getOldestKey();

        T oldestFromDataStorage = getOldestFromDataStorage();
        return oldestFromDataStorage != null ? oldestFromDataStorage.getKey() : -1L;
    }


    private T getOldestFromDataStorage() {
        try {
            if (_externalStorage.isEmpty()) {
                throw new NoSuchElementException();
            }
            StorageReadOnlyIterator<T> storageIterator = _externalStorage.readOnlyIterator(0);
            T oldest = storageIterator.next();
            storageIterator.close();
            return oldest;
        } catch (StorageException e) {
            throw new SwapStorageException(e);
        }
    }

    @Override
    public boolean isEmpty() {
        //return true if both the memory redo log file is empty and the external storage
        try {
            return _memoryRedoLogFile.isEmpty() && (_insertToExternal ? _externalStorage.isEmpty() : true);
        } catch (StorageException e) {
            throw new SwapStorageException(e);
        }
    }

    @Override
    public ReadOnlyIterator<T> readOnlyIterator(long fromKey) {
        if (isEmpty()) {
            return _memoryRedoLogFile.readOnlyIterator(fromKey);
        }
        final long key = getOldest().getKey();
        final long firstKeyInBacklog = key < 0 ? 0 : key;
        final long fromIndex = Math.max(0, fromKey - firstKeyInBacklog);
        final long memRedoFileSize = _memoryRedoLogFile.size();
        if (fromIndex < memRedoFileSize)
            return new SwapReadOnlyIterator(_memoryRedoLogFile.readOnlyIterator(fromKey));
        //Skip entire memory redo log, can safely cast to int because here memRedoFileSize cannot be more than int
        return new SwapReadOnlyIterator(fromIndex - memRedoFileSize);
    }

    @Override
    public T removeOldest() {
        if (!_memoryRedoLogFile.isEmpty())
            return _memoryRedoLogFile.removeOldest();

        moveOldestBatchFromStorage();

        return _memoryRedoLogFile.removeOldest();
    }

    @Override
    public long size() {
        //Returns the size of the redo log file taking both the swapped packets
        //and the memory residing packets into consideration
        try {
            return _memoryRedoLogFile.size() + (_insertToExternal ? _externalStorage.size() : 0);
        } catch (StorageException e) {
            throw new SwapStorageException(e);
        }
    }

    @Override
    public long getApproximateSize() {
        try {
            return _memoryRedoLogFile.getApproximateSize() + _externalStorage.size();
        } catch (StorageException e) {
            throw new SwapStorageException(e);
        }
    }

    @Override
    public void deleteOldestPackets(long packetsCount) {
        long memorySize = _memoryRedoLogFile.size();
        _memoryRedoLogFile.deleteOldestPackets(packetsCount);

        if (memorySize < packetsCount)
            deleteOldestPacketsFromStorage(packetsCount - memorySize);

        if (_memoryRedoLogFile.isEmpty() && _insertToExternal)
            moveOldestBatchFromStorage();
    }

    private void deleteOldestPacketsFromStorage(long packetsCount) {
        try {
            _externalStorage.deleteOldestPackets(packetsCount);
        } catch (StorageException e) {
            throw new SwapStorageException(e);
        }
    }

    private void moveOldestBatchFromStorage() {
        try {
            WeightedBatch<T> batch = _externalStorage.removeFirstBatch(_fetchBatchCapacity, _lastCompactionRangeEndKey);
            if (_logger.isTraceEnabled())
                _logger.trace("Moved a batch of packets from storage into memory, batch weight is " + batch.getWeight());

            if (batch.getWeight() + getCacheSize() > _combinedMemoryMaxCapacity) {
                _logger.warn("Moved a batch of packets from storage into memory which weight causes a breach of memory max capacity," +
                        " batch weight: " + batch.getWeight() + ", current memory weight: " + getCacheSize() + "\n");
            }

            for (T packet : batch.getBatch())
                _memoryRedoLogFile.add(packet);
            if (_externalStorage.isEmpty() && batch.getWeight() < _memoryMaxCapacity)
                _insertToExternal = false;

            if (!batch.getCompactionResult().isEmpty()) {
                _groupBacklog.updateMirrorWeightAfterCompaction(batch.getCompactionResult());
            }

        } catch (StorageException e) {
            throw new SwapStorageException(e);
        }
    }

    private long getCacheSize() {
        return _externalStorage.getCacheWeight();
    }

    private void addToStorage(T replicationPacket) {
        try {
            _externalStorage.append(replicationPacket);
        } catch (StorageException e) {
            throw new SwapStorageException(e);
        }
    }

    public long getMemoryPacketCount() {
        return _memoryRedoLogFile.size();
    }

    public long getStoragePacketCount() {
        try {
            return _externalStorage.size();
        } catch (StorageException e) {
            throw new SwapStorageException(e);
        }
    }

    @Override
    public long getExternalStorageSpaceUsed() {
        return _externalStorage.getSpaceUsed();
    }

    @Override
    public long getExternalStoragePacketsCount() {
        return _externalStorage.getExternalPacketsCount();
    }

    @Override
    public long getMemoryPacketsWeight() {
        return _memoryRedoLogFile.getMemoryPacketsWeight() + _externalStorage.getMemoryPacketsWeight();
    }

    @Override
    public long getExternalStoragePacketsWeight() {
        return _externalStorage.getExternalStoragePacketsWeight();
    }

    @Override
    public long getMemoryPacketsCount() {
        return _memoryRedoLogFile.getMemoryPacketsCount() + _externalStorage.getMemoryPacketsCount();
    }

    @Override
    public void validateIntegrity() throws RedoLogFileCompromisedException {
        _memoryRedoLogFile.validateIntegrity();
        _externalStorage.validateIntegrity();
    }

    @Override
    public void close() {
        _memoryRedoLogFile.close();
        _externalStorage.close();
    }

    @Override
    public long getWeight() {
        return _memoryRedoLogFile.getWeight() + _externalStorage.getWeight();
    }

    @Override
    public CompactionResult performCompaction(long from, long to) {
        final CompactionResult result = new CompactionResult();
        if (_lastCompactionRangeEndKey != -1) {

            from = _lastCompactionRangeEndKey + 1;
        }


        if (to - from < ReplicationPolicy.DEFAULT_REDO_LOG_COMPACTION_BATCH_SIZE) {
            return result;
        }

        if (from > _lastSeenTransientPacketKey) {
            if (_logger.isTraceEnabled()) {
                _logger.debug("[" + _name + "]: No transient packets in range " + from + "-" + to + ", lastSeenTransientPacketKey = " + _lastSeenTransientPacketKey);
            }
            return result;
        }

        if (_logger.isDebugEnabled()) {
            _logger.debug("[" + _name + "]: Performing Compaction " + from + "-" + to);
        }

        result.appendResult(_memoryRedoLogFile.performCompaction(from, to));
        result.appendResult(_externalStorage.performCompaction(from, to));

        if (_logger.isDebugEnabled()) {
            _logger.debug("[" + _name + "]: Discarded of " + result.getDiscardedCount() + " packets and deleted " + result.getDeletedFromTxn() + " transient packets from transactions during compaction process");
        }

        _lastCompactionRangeEndKey = to;

        return result;
    }

    /**
     * A read only iterator which iterate over the memory redo log file, and once completed
     * iterating over it, it continue to iterate over the external storage
     *
     * @author eitany
     * @since 7.1
     */
    private class SwapReadOnlyIterator
            implements ReadOnlyIterator<T> {

        private final ReadOnlyIterator<T> _memoryIterator;
        private boolean _memoryIteratorExhausted = false;
        private StorageReadOnlyIterator<T> _externalIterator = null;

        /**
         * Create an iterator which stars iterating over the packets which reside in the memory redo
         * log file
         */
        public SwapReadOnlyIterator(
                ReadOnlyIterator<T> memoryIterator) {
            this._memoryIterator = memoryIterator;
        }


        /**
         * Create an iterator which starts directly iterating over the storage, thus skipping the
         * memory redo log file
         *
         * @param inSwapStartIndex offset index to start inside the storage
         */
        public SwapReadOnlyIterator(long inSwapStartIndex) {
            _memoryIteratorExhausted = true;
            _memoryIterator = null;
            try {
                _externalIterator = _externalStorage.readOnlyIterator(inSwapStartIndex);
            } catch (StorageException e) {
                throw new SwapStorageException(e);
            }
        }


        public boolean hasNext() {
            if (!_memoryIteratorExhausted) {
                _memoryIteratorExhausted = !_memoryIterator.hasNext();
                if (!_memoryIteratorExhausted)
                    return true;
            }

            try {
                //If here, memory iterator is exhausted
                if (_externalIterator == null)
                    _externalIterator = _externalStorage.readOnlyIterator(0);

                return _externalIterator.hasNext();
            } catch (StorageException e) {
                throw new SwapStorageException(e);
            }
        }

        public T next() {
            if (!_memoryIteratorExhausted) {
                try {
                    return _memoryIterator.next();
                } catch (NoSuchElementException e) {
                    _memoryIteratorExhausted = true;
                }
            }

            try {
                //If here, memory iterator is exhausted (support iteration using only next())
                if (_externalIterator == null)
                    _externalIterator = _externalStorage.readOnlyIterator(0);

                return _externalIterator.next();
            } catch (StorageException e) {
                throw new SwapStorageException(e);
            }
        }


        public void close() {
            if (_memoryIterator != null)
                _memoryIterator.close();
            if (_externalIterator != null)
                try {
                    _externalIterator.close();
                } catch (StorageException e) {
                    throw new SwapStorageException(e);
                }
        }

    }

    public MemoryRedoLogFile<T> getMemoryRedoLogFile() {
        return _memoryRedoLogFile;
    }
}
