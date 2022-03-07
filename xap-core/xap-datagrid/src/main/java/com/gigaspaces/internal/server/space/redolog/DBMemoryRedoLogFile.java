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
import java.util.LinkedList;

import static com.gigaspaces.logger.Constants.LOGGER_REPLICATION_BACKLOG;

public class DBMemoryRedoLogFile<T extends IReplicationOrderedPacket> implements IRedoLogFile<T> {

    final private LinkedList<T> _redoFile = new LinkedList<>();
    private final String _name;
    private final AbstractSingleFileGroupBacklog _groupBacklog;
    private long _weight;
    private long _discardedPacketCount;
    private final Logger _logger;

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
        return _redoFile.removeFirst();
    }

    @Override
    public T getOldest() {
        return _redoFile.getFirst();
    }

    @Override
    public void add(T replicationPacket) {
        _redoFile.addLast(replicationPacket);
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
                removeOldest();
                // decreaseWeight(first); TODO
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
        final CompactionResult compactionResult =RedoLogCompactionUtil.compact(from, to, _redoFile.listIterator());
        this._weight -= compactionResult.getDiscardedCount() + compactionResult.getDeletedFromTxn();
        this._discardedPacketCount += compactionResult.getDiscardedCount();
        return compactionResult;
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
        return _redoFile.iterator();
    }

    public Iterator<T> iterator(long fromKey) {
        if (isEmpty()) return _redoFile.iterator();

        //fromKey = 25, filter: 25 <= k <= newest
        //oldest->[21, 22, 23, 24, 25, 26, 27, ..., 80]->newest
        long oldestKey = getOldest().getKey(); //25
        int fromIndex = (int)(fromKey - oldestKey); // index = (25 - 21) =4
        return _redoFile.subList(fromIndex, _redoFile.size()).iterator();

//TODO the above code assumes that the keys are +1
// if it turns out not to be the case, we might need to go over and find the first index:
//        int fromIndex = 0;
//        for (T t : _redoFile) {
//            if (t.getKey() == fromKey) {
//                break;
//            } else {
//                ++fromIndex;
//            }
//        }
    }
}
