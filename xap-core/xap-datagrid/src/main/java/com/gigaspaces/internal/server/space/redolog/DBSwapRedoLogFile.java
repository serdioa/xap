package com.gigaspaces.internal.server.space.redolog;

import com.gigaspaces.internal.cluster.node.impl.backlog.AbstractSingleFileGroupBacklog;
import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.server.space.redolog.storage.IRedoLogFileStorage;
import com.gigaspaces.internal.server.space.redolog.storage.SqliteRedoLogFileStorage;
import com.gigaspaces.internal.server.space.redolog.storage.StorageException;
import com.gigaspaces.internal.server.space.redolog.storage.bytebuffer.WeightedBatch;
import com.gigaspaces.internal.utils.collections.ReadOnlyIterator;
import com.gigaspaces.logger.Constants;
import com.j_spaces.core.cluster.ReplicationPolicy;
import com.j_spaces.core.cluster.startup.CompactionResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;

public class DBSwapRedoLogFile<T extends IReplicationOrderedPacket> implements IRedoLogFile<T> {

    private static final Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_REPLICATION_BACKLOG);

    private final IRedoLogFile<T> _memoryRedoLog;
    private final IRedoLogFileStorage<T> _externalRedoLogStorage;
    private final DBSwapRedoLogFileConfig<T> _config;

    private final AbstractSingleFileGroupBacklog _groupBacklog;
    private long _lastCompactionRangeEndKey = -1;
    private long _lastSeenTransientPacketKey = -1;

    public DBSwapRedoLogFile(DBSwapRedoLogFileConfig<T> config,
                             AbstractSingleFileGroupBacklog groupBacklog) {
        this._memoryRedoLog = new DBMemoryRedoLogFile<T>(config, groupBacklog);
        this._externalRedoLogStorage = new SqliteRedoLogFileStorage<T>(config);
        this._config = config;
        _groupBacklog = groupBacklog;
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
        //todo: use weight
        final int flushPacketSize = (int) Math.min(_memoryRedoLog.size(), _config.getFlushBufferPacketCount());
        ArrayList<T> batchToFlush = new ArrayList<>(flushPacketSize);
        if (_memoryRedoLog.size() > _config.getMemoryPacketCapacity()) {
            for (int i = 0; i < flushPacketSize; i++) {
                T oldest = _memoryRedoLog.removeOldest();
                if (oldest != null) { //todo: check if needed
                    batchToFlush.add(oldest);
                }
            }
            try {
                _externalRedoLogStorage.appendBatch(batchToFlush);
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
            if (_externalRedoLogStorage.isEmpty()) {
                T oldest = _memoryRedoLog.removeOldest();
                return oldest == null ? removeOldest() : oldest; //todo limited retry attempts
            }
            WeightedBatch<T> tWeightedBatch = _externalRedoLogStorage.removeFirstBatch(1, _lastCompactionRangeEndKey);//todo : check _lastCompactionRangeEndKey
            return tWeightedBatch.getBatch().get(0);
        } catch (StorageException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public T getOldest() {
        try {
            if (_externalRedoLogStorage.isEmpty()) {
                T oldest = _memoryRedoLog.getOldest();
                return oldest == null ? getOldest() : oldest; //todo limited retry attempts
            }
            return _externalRedoLogStorage.getOldest();
        } catch (StorageException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public long size() {
        try {
            return _externalRedoLogStorage.size() + _memoryRedoLog.size();
        } catch (StorageException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public long getApproximateSize() {
        try {
            return _externalRedoLogStorage.size() + _memoryRedoLog.size();
        } catch (StorageException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public boolean isEmpty() {
        try {
            return _externalRedoLogStorage.isEmpty() && _memoryRedoLog.isEmpty();
        } catch (StorageException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public void deleteOldestPackets(long packetsCount) {
        try {
            if (_externalRedoLogStorage.isEmpty()) {
                _memoryRedoLog.deleteOldestPackets(packetsCount);
                return;
            }
            WeightedBatch<T> tWeightedBatch = _externalRedoLogStorage.removeFirstBatch((int) packetsCount, _lastCompactionRangeEndKey);//todo : check _lastCompactionRangeEndKey
            long packetsRemaining = packetsCount - tWeightedBatch.size();
            if (packetsRemaining > 0) {
                _memoryRedoLog.deleteOldestPackets(packetsRemaining);
            }
        } catch (StorageException e) {
            throw new IllegalArgumentException(e);
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
        return _externalRedoLogStorage.getDiscardedPacketsCount() + _memoryRedoLog.getDiscardedPacketsCount();
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
                _logger.debug("[" + _config.getSpaceName() + "]: No transient packets in range " + from + "-" + to + ", lastSeenTransientPacketKey = " + _lastSeenTransientPacketKey);
            }
            return result;
        }

        if (_logger.isDebugEnabled()) {
            _logger.debug("[" + _config.getSpaceName() + "]: Performing Compaction " + from + "-" + to);
        }

        result.appendResult(_memoryRedoLog.performCompaction(from, to));
        //TODO: @sagiv currently we not support it
//        result.appendResult(_externalStorageRedoLog.performCompaction(from, to));

        if (_logger.isDebugEnabled()) {
            _logger.debug("[" + _config.getSpaceName() + "]: Discarded of " + result.getDiscardedCount() + " packets and deleted " + result.getDeletedFromTxn() + " transient packets from transactions during compaction process");
        }

        _lastCompactionRangeEndKey = to;

        return result;
    }

    @Override
    public ReadOnlyIterator<T> readOnlyIterator() {
        try {
            if (_externalRedoLogStorage.isEmpty()) {
                return _memoryRedoLog.readOnlyIterator();
            }
            return _externalRedoLogStorage.readOnlyIterator();
        } catch (StorageException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public ReadOnlyIterator<T> readOnlyIterator(long fromKey) {
        if (!_memoryRedoLog.isEmpty() && _memoryRedoLog.getOldest().getKey() <= fromKey) {
            return _memoryRedoLog.readOnlyIterator(fromKey);
        }
        if (_externalRedoLogStorage.isEmpty()) {
            return _memoryRedoLog.readOnlyIterator();
        }
        return _externalRedoLogStorage.readOnlyIterator(fromKey);
    }

    @Override
    public Iterator<T> iterator() {
        return _memoryRedoLog.iterator();
    }
 }
