package com.gigaspaces.internal.server.space.redolog;

import com.gigaspaces.internal.cluster.node.impl.backlog.AbstractSingleFileGroupBacklog;
import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.server.space.redolog.storage.IRedoLogFileStorage;
import com.gigaspaces.internal.server.space.redolog.storage.SqliteRedoLogFileStorage;
import com.gigaspaces.internal.utils.collections.ReadOnlyIterator;
import com.j_spaces.core.cluster.ReplicationPolicy;
import com.j_spaces.core.cluster.startup.CompactionResult;
import com.j_spaces.core.cluster.startup.RedoLogCompactionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static com.gigaspaces.logger.Constants.LOGGER_REPLICATION_BACKLOG;

public class DBSwapRedoLogFile<T extends IReplicationOrderedPacket> implements IRedoLogFile<T> {

    private final Logger _logger;

    private final IRedoLogFile<T> _memoryRedoLog;
    private final IRedoLogFileStorage<T> _externalRedoLogStorage;
    private final DBSwapRedoLogFileConfig<T> _config;

    private final AbstractSingleFileGroupBacklog<?, ?> _groupBacklog;
    private long _lastCompactionRangeEndKey = -1;
    private long _lastSeenTransientPacketKey = -1;

    public DBSwapRedoLogFile(DBSwapRedoLogFileConfig<T> config,
                             AbstractSingleFileGroupBacklog<?, ?> groupBacklog) {
        this._logger = LoggerFactory.getLogger(LOGGER_REPLICATION_BACKLOG + "." + config.getFullMemberName());
        _logger.info("Creating swap redo-log - configuration: " + config);
        this._memoryRedoLog = new DBMemoryRedoLogFile<T>(config, groupBacklog);
        this._externalRedoLogStorage = new SqliteRedoLogFileStorage<T>(config);
        this._config = config;
        this._groupBacklog = groupBacklog;
    }

    @Override
    public long getExternalStorageSpaceUsed() {
        return _externalRedoLogStorage.getSpaceUsed();
    }

    @Override
    public long getMemoryPacketsCount() {
        return _memoryRedoLog.getMemoryPacketsCount();
    }

    @Override
    public long getExternalStoragePacketsCount() {
        return _externalRedoLogStorage.getExternalPacketsCount();
    }

    @Override
    public long getMemoryPacketsWeight() {
        return _memoryRedoLog.getMemoryPacketsWeight();
    }

    @Override
    public long getExternalStoragePacketsWeight() {
        return _externalRedoLogStorage.getExternalStoragePacketsWeight();
    }


    @Override
    public void add(T replicationPacket) {
        _memoryRedoLog.add(replicationPacket);
        //don't move this block into the 'if' - major degradation in performance
        final long weight = _memoryRedoLog.getWeight();
        final int numberOfPacketsToRemove = (int) Math.min(weight, Math.min(_memoryRedoLog.size(), _config.getFlushBufferPacketCount()));
        final ArrayList<T> packetsRemoved = new ArrayList<>(numberOfPacketsToRemove);
        //end of block
        if (weight > _config.getMemoryPacketCapacity()) {
            int batchWeight = 0;
            for (int i = 0; i < numberOfPacketsToRemove; i++) {
                T oldest = _memoryRedoLog.removeOldest();
                packetsRemoved.add(oldest);
                batchWeight += oldest.getWeight();
                if (batchWeight > numberOfPacketsToRemove){
                    break;
                }
            }
            //move packets from memory to external storage
            _externalRedoLogStorage.appendBatch(packetsRemoved);
        }
        if (RedoLogCompactionUtil.isCompactable(replicationPacket)) {
            _lastSeenTransientPacketKey = replicationPacket.getKey();
        }
    }

    @Override
    public T removeOldest() {
        if (_externalRedoLogStorage.isEmpty()) {
            if (_memoryRedoLog.isEmpty()) {
                throw new NoSuchElementException();
            }
            return _memoryRedoLog.removeOldest();
        }
        return _externalRedoLogStorage.removeOldest();
    }

    @Override
    public T getOldest() {
        if (_externalRedoLogStorage.isEmpty()) {
            if (_memoryRedoLog.isEmpty()) {
                throw new NoSuchElementException();
            }
            return _memoryRedoLog.getOldest();
        }
        return _externalRedoLogStorage.getOldest();
    }

    @Override
    public long size() {
        return _externalRedoLogStorage.size() + _memoryRedoLog.size();
    }

    @Override
    public long getApproximateSize() {
        return _externalRedoLogStorage.size() + _memoryRedoLog.size();
    }

    @Override
    public boolean isEmpty() {
        return _externalRedoLogStorage.isEmpty() && _memoryRedoLog.isEmpty();
    }

    @Override
    public void deleteOldestPackets(long packetsCount) {
        if (_externalRedoLogStorage.isEmpty()) {
            _memoryRedoLog.deleteOldestPackets(packetsCount);
            return;
        }
        long beforeDeletionPacketCount = _externalRedoLogStorage.getExternalPacketsCount();
        _externalRedoLogStorage.deleteOldestPackets(packetsCount);
        long afterDeletionPacketCount = _externalRedoLogStorage.getExternalPacketsCount();
        long packetsRemaining = packetsCount - (beforeDeletionPacketCount - afterDeletionPacketCount);
        if (packetsRemaining > 0) {
            _memoryRedoLog.deleteOldestPackets(packetsRemaining);
        }
    }

    @Override
    public void validateIntegrity() throws RedoLogFileCompromisedException {
        _memoryRedoLog.validateIntegrity();
        _externalRedoLogStorage.validateIntegrity();
    }

    @Override
    public void close() {
        _memoryRedoLog.close();
        _externalRedoLogStorage.close();
    }

    @Override
    public long getWeight() {
        return _externalRedoLogStorage.getWeight() + _memoryRedoLog.getWeight();
    }

    @Override
    public long getDiscardedPacketsCount() {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompactionResult performCompaction(long from, long to) {
        if (_lastCompactionRangeEndKey != -1) {
            from = _lastCompactionRangeEndKey + 1;
        }

        if (to - from < ReplicationPolicy.DEFAULT_REDO_LOG_COMPACTION_BATCH_SIZE) {
            return new CompactionResult(); //empty
        }

        if (from > _lastSeenTransientPacketKey) {
            if (_logger.isDebugEnabled()) {
                _logger.debug("[" + _config.getFullMemberName() + "]: No transient packets in range "
                        + from + "-" + to + ", lastSeenTransientPacketKey = " + _lastSeenTransientPacketKey);
            }
            return new CompactionResult(); //empty
        }

        if (_logger.isDebugEnabled()) {
            _logger.debug("[" + _config.getFullMemberName() + "]: Performing Compaction " + from + "-" + to);
        }

        // we perform compaction only on memory redo-log
        CompactionResult compactionResult = _memoryRedoLog.performCompaction(from, to);

        if (_logger.isDebugEnabled()) {
            _logger.debug("[" + _config.getFullMemberName()
                    + "]: transient packets compacted =" + compactionResult.getDiscardedCount()
                    + ", transient packets removed from txn =" + compactionResult.getDeletedFromTxn());
        }

        //we replace transient packet with discarded packet that have the same weight
        compactionResult.setDiscardedCount(0);

        // we use this range to compact while iterating over external storage
        // see ExternalStorageCompactionReadOnlyIterator
        _lastCompactionRangeEndKey = to;

        return compactionResult;
    }

    @Override
    public ReadOnlyIterator<T> readOnlyIterator(long fromKey) {
        if (!_memoryRedoLog.isEmpty() && _memoryRedoLog.getOldest().getKey() <= fromKey) {
            return _memoryRedoLog.readOnlyIterator(fromKey);
        }
        if (_externalRedoLogStorage.isEmpty()) {
            return _memoryRedoLog.readOnlyIterator(-1);
        }
        return new ExternalStorageCompactionReadOnlyIterator(
                _externalRedoLogStorage.readOnlyIterator(fromKey),
                _lastCompactionRangeEndKey);
    }

    @Override
    public Iterator<T> iterator() {
        //todo: remove from interface!!!!!!
        return _memoryRedoLog.iterator();
    }


    private class ExternalStorageCompactionReadOnlyIterator implements ReadOnlyIterator<T>{

        private final ReadOnlyIterator<T> iterator;
        private final long lastCompactionKey;

        public ExternalStorageCompactionReadOnlyIterator(ReadOnlyIterator<T> iterator, long lastCompactionKey){
            this.iterator = iterator;
            this.lastCompactionKey = lastCompactionKey;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public T next() {
            return (T) RedoLogCompactionUtil.compactPacket(iterator.next(), lastCompactionKey);
        }

        @Override
        public void close() {
            iterator.close();
        }
    }
}