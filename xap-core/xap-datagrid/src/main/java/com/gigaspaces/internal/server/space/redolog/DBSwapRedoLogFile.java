package com.gigaspaces.internal.server.space.redolog;

import com.gigaspaces.internal.cluster.node.impl.backlog.AbstractSingleFileGroupBacklog;
import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.server.space.redolog.storage.IRedoLogFileStorage;
import com.gigaspaces.internal.server.space.redolog.storage.StorageException;
import com.gigaspaces.internal.server.space.redolog.storage.StorageReadOnlyIterator;
import com.gigaspaces.internal.server.space.redolog.storage.bytebuffer.WeightedBatch;
import com.gigaspaces.internal.utils.collections.ReadOnlyIterator;
import com.gigaspaces.logger.Constants;
import com.j_spaces.core.cluster.ReplicationPolicy;
import com.j_spaces.core.cluster.startup.CompactionResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;

public class DBSwapRedoLogFile<T extends IReplicationOrderedPacket> implements IRedoLogFile<T> {

    private static final Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_REPLICATION_BACKLOG);

    private final int _memoryPacketCapacity;
    private final int _diskPacketCapacity;
    private final int _flushPacketSize;
    private long lastMemoryRedoKey;
    private LinkedList<T> externalFirstBatch = new LinkedList<>(); //todo: maybe remove

    private final DBMemoryRedoLogFile<T> _memoryRedoLogFile;
    private final IRedoLogFileStorage<T> _externalRedoLogStorage;
    private final String _name;
    private final AbstractSingleFileGroupBacklog _groupBacklog;
    //Not volatile because this is not a thread safe structure, assume flushing of thread cache
    //changes because lock is held at upper layer
    private boolean _insertToExternal = false;
    private long _lastCompactionRangeEndKey = -1;
    private long _lastSeenTransientPacketKey = -1;

    public DBSwapRedoLogFile(String name, DBSwapRedoLogFileConfig<T> config, AbstractSingleFileGroupBacklog groupBacklog) {
        _memoryPacketCapacity = config.getMemoryPacketCapacity();
        _diskPacketCapacity = config.getDiskPacketCapacity();
        _flushPacketSize = config.getFlushPacketSize();
        _memoryRedoLogFile = new DBMemoryRedoLogFile<T>(name, groupBacklog);
        _externalRedoLogStorage = config.getRedoLogFileStorage();
        _name = name;
        _groupBacklog = groupBacklog;
    }

    @Override
    public long getExternalStorageSpaceUsed() {
        return _externalRedoLogStorage.getSpaceUsed();
    }

    @Override
    public long getMemoryPacketsCount() {
        return _memoryRedoLogFile.getMemoryPacketsCount();
    }

    @Override
    public long getExternalStoragePacketsCount() {
        return _externalRedoLogStorage.getExternalPacketsCount();
    }

    @Override
    public long getMemoryPacketsWeight() {
        return _memoryRedoLogFile.getMemoryPacketsWeight();
    }

    @Override
    public long getExternalStoragePacketsWeight() {
        return _externalRedoLogStorage.getExternalStoragePacketsWeight();
    }


    @Override
    public void add(T replicationPacket) {
        _memoryRedoLogFile.add(replicationPacket);
        //todo: use weight
        ArrayList<T> rpToFlush = new ArrayList<>(_flushPacketSize);
        if (_memoryRedoLogFile.size() > _memoryPacketCapacity) {
            for (int i = 0; i < _flushPacketSize; i++) {
                T oldest = _memoryRedoLogFile.removeOldest();
                if (oldest != null) { //todo: check if needed
                    rpToFlush.add(oldest);
                }
            }
            try {
                _externalRedoLogStorage.appendBatch(rpToFlush);
                lastMemoryRedoKey += rpToFlush.size(); //todo: maybe use flushPacketSize
            } catch (StorageException e) {
                throw new IllegalArgumentException(e);
            }
        }
        //        if (RedoLogCompactionUtil.isCompactable(replicationPacket)) {
        //            _lastSeenTransientPacketKey = replicationPacket.getKey();
        //        }
    }

    @Override
    public T removeOldest() {
        try {
            if (_externalRedoLogStorage.isEmpty() && externalFirstBatch.isEmpty()) {
                T oldest = _memoryRedoLogFile.removeOldest();
                return oldest == null ? removeOldest() : oldest; //todo limited retry attempts
            }
            if (!externalFirstBatch.isEmpty()) {
                return externalFirstBatch.removeFirst();
            }
            WeightedBatch<T> tWeightedBatch = _externalRedoLogStorage.removeFirstBatch(1, _lastCompactionRangeEndKey);//todo : check _lastCompactionRangeEndKey
            externalFirstBatch.addAll(tWeightedBatch.getBatch());
            return externalFirstBatch.removeFirst();
        } catch (StorageException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public T getOldest() {
        try {
            if (_externalRedoLogStorage.isEmpty() && externalFirstBatch.isEmpty()) {
                T oldest = _memoryRedoLogFile.getOldest();
                return oldest == null ? getOldest() : oldest; //todo limited retry attempts
            }
            if (!externalFirstBatch.isEmpty()) {
                return externalFirstBatch.getFirst();
            }

            //todo: maybe use readOnlyItereator
            WeightedBatch<T> tWeightedBatch = _externalRedoLogStorage.removeFirstBatch(1, _lastCompactionRangeEndKey);//todo : check _lastCompactionRangeEndKey
            externalFirstBatch.addAll(tWeightedBatch.getBatch());
            return externalFirstBatch.getFirst();
        } catch (StorageException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public long size() {
        try {
            return _externalRedoLogStorage.size() + _memoryRedoLogFile.size();
        } catch (StorageException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public long getApproximateSize() {
        try {
            return _externalRedoLogStorage.size() + _memoryRedoLogFile.size();
        } catch (StorageException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public boolean isEmpty() {
        try {
            return _externalRedoLogStorage.isEmpty() && _memoryRedoLogFile.isEmpty();
        } catch (StorageException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public void deleteOldestPackets(long packetsCount) {
        for (int i = 0; i < packetsCount; i++) {
            removeOldest();
        }
    }

    @Override
    public void validateIntegrity() throws RedoLogFileCompromisedException {
        _memoryRedoLogFile.validateIntegrity();
        _externalRedoLogStorage.validateIntegrity();
    }

    @Override
    public void close() {
        _memoryRedoLogFile.close();
        _externalRedoLogStorage.close();
    }

    @Override
    public long getWeight() {
        return _externalRedoLogStorage.getWeight() + _memoryRedoLogFile.getWeight();
    }

    @Override
    public long getDiscardedPacketsCount() {
        return _externalRedoLogStorage.getDiscardedPacketsCount() + _memoryRedoLogFile.getDiscardedPacketsCount();
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
        result.appendResult(_externalRedoLogStorage.performCompaction(from, to));

        if (_logger.isDebugEnabled()) {
            _logger.debug("[" + _name + "]: Discarded of " + result.getDiscardedCount() + " packets and deleted " + result.getDeletedFromTxn() + " transient packets from transactions during compaction process");
        }

        _lastCompactionRangeEndKey = to;

        return result;
    }

    @Override
    public ReadOnlyIterator<T> readOnlyIterator() {
        try {
            if (_externalRedoLogStorage.isEmpty() && externalFirstBatch.isEmpty()) {
                return _memoryRedoLogFile.readOnlyIterator();
            }
            return new DBSwapIterator();
        } catch (StorageException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public ReadOnlyIterator<T> readOnlyIterator(long fromKey) {
        try {
            if (_externalRedoLogStorage.isEmpty() && externalFirstBatch.isEmpty()) {
                return _memoryRedoLogFile.readOnlyIterator(fromKey);
            }
            return new DBSwapIterator(fromKey);
        } catch (StorageException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public Iterator<T> iterator() {
        return _memoryRedoLogFile.iterator();
    }

    private class DBSwapIterator implements ReadOnlyIterator<T> {

        private ReadOnlyIterator<T> memoryIter;
        private StorageReadOnlyIterator<T> storageIter;
        private boolean fromDisk = false;
        private boolean fromMemory = false;

        public DBSwapIterator() throws StorageException {
            this.memoryIter = _memoryRedoLogFile.readOnlyIterator();
            this.storageIter = _externalRedoLogStorage.readOnlyIterator();
        }

        public DBSwapIterator(long fromKey) throws StorageException {
            this.memoryIter = _memoryRedoLogFile.readOnlyIterator();
            this.storageIter = _externalRedoLogStorage.readOnlyIterator(fromKey);
        }

        @Override
        public boolean hasNext() {
            try {
                if (storageIter.hasNext()) {
                    fromDisk = true;
                    fromMemory = false;
                    return true;
                }
            } catch (StorageException e) {
                throw new IllegalArgumentException(e);
            }
            if (memoryIter.hasNext()) {
                fromDisk = false;
                fromMemory = true;
                return true;
            }
            return false;
        }

        @Override
        public T next() {
            if (fromMemory) {
                return memoryIter.next();
            }
            if (fromDisk) {
                try {
                    return storageIter.next();
                } catch (StorageException e) {
                    throw new IllegalArgumentException(e);
                }
            }
            return null;
        }

        @Override
        public void close() {
            memoryIter.close();
            try {
                storageIter.close();
            } catch (StorageException e) {
                throw new IllegalArgumentException(e);
            }
        }
    }
}
