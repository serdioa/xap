package com.gigaspaces.internal.server.space.redolog;

import com.gigaspaces.internal.cluster.node.impl.backlog.AbstractSingleFileGroupBacklog;
import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.utils.collections.ReadOnlyIterator;
import com.gigaspaces.internal.utils.collections.ReadOnlyIteratorAdapter;
import com.j_spaces.core.cluster.startup.CompactionResult;
import com.j_spaces.core.cluster.startup.RedoLogCompactionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

import static com.gigaspaces.logger.Constants.LOGGER_REPLICATION_BACKLOG;

public class DBMemoryRedoLogFile<T extends IReplicationOrderedPacket> implements IRedoLogFile<T> {

    final private ConcurrentHashMap<Long, T> _redoFile = new ConcurrentHashMap<>();
    private final String _name;
    private final AbstractSingleFileGroupBacklog _groupBacklog;
    private long _weight;
    private long _discardedPacketCount;
    private final Logger _logger;
    private volatile long oldestPacketInMemory;

    public DBMemoryRedoLogFile(String name, AbstractSingleFileGroupBacklog groupBacklog) {
        _name = name;
        _groupBacklog = groupBacklog;
        _logger = LoggerFactory.getLogger(LOGGER_REPLICATION_BACKLOG + "." + _name);
    }

    @Override
    public long getExternalStorageSpaceUsed() {
        //memory only redo log
        return 0;
    }

    @Override
    public long getMemoryPacketsCount() {
        return size();
    }

    @Override
    public long getExternalStoragePacketsCount() {
        //memory only redo log
        return 0;
    }

    @Override
    public long getMemoryPacketsWeight() {
        return getWeight();
    }

    @Override
    public long getExternalStoragePacketsWeight() {
        //memory only redo log
        return 0;
    }

    @Override
    public T removeOldest() {
        return _redoFile.remove(oldestPacketInMemory++);
    }

    @Override
    public T getOldest() {
        return _redoFile.get(oldestPacketInMemory);
    }

    @Override
    public void add(T replicationPacket) {
        boolean isEmpty = isEmpty();
        _redoFile.put(replicationPacket.getKey(), replicationPacket);
        if (isEmpty){
            oldestPacketInMemory = _redoFile.keySet().stream().min(Long::compare).get();
        }
        //todo : increase weight
    }

    @Override
    public long size() {
        return _redoFile.size();
    }

    @Override
    public long getApproximateSize() {
        return size();
    }

    @Override
    public boolean isEmpty() {
        return _redoFile.isEmpty();
    }

    @Override
    public void deleteOldestPackets(long packetsCount) {
        if (packetsCount > size()) {
            _redoFile.clear();
            _weight = 0;
            _discardedPacketCount = 0;
        } else {
            for (long i = 0; i < packetsCount; ++i) {
                T first = _redoFile.remove(oldestPacketInMemory);
                oldestPacketInMemory++;
//                decreaseWeight(first);
            }
        }
    }

    @Override
    public void validateIntegrity() throws RedoLogFileCompromisedException {
        //Memory redo log cannot be compromised
    }

    @Override
    public void close() {
        _redoFile.clear();
        _weight = 0;
        _discardedPacketCount = 0;
    }

    @Override
    public long getWeight() {
        return RedoLogCompactionUtil.calculateWeight(_weight, _discardedPacketCount);
    }

    @Override
    public long getDiscardedPacketsCount() {
        return _discardedPacketCount;
    }

    @Override
    public CompactionResult performCompaction(long from, long to) {
        //TODO
        return null;
    }

    @Override
    public ReadOnlyIterator<T> readOnlyIterator() {
        return new ReadOnlyIteratorAdapter<>(iterator());
    }

    @Override
    public ReadOnlyIterator<T> readOnlyIterator(long fromKey) {
        return new ReadOnlyIteratorAdapter<>(iterator(fromKey));
    }

    @Override
    public Iterator<T> iterator() {
        return iterator(oldestPacketInMemory);
    }

    public Iterator<T> iterator(long startingIndex) {
        return new Iterator<T>() {
            long currentIndex = startingIndex;
            T next = null;

            @Override
            public boolean hasNext() {
                next = _redoFile.get(currentIndex);
                return next != null;
            }

            @Override
            public T next() {
                currentIndex++;
                return next;
            }
        };
    }
}
