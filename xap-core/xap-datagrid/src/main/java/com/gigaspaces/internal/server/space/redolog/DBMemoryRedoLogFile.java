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

    private final Logger _logger;
    private final AbstractSingleFileGroupBacklog<?, ?> _groupBacklog;
    final private LinkedList<T> _redoFile = new LinkedList<>();
    private long _weight;

    public DBMemoryRedoLogFile(DBSwapRedoLogFileConfig<T> config, AbstractSingleFileGroupBacklog<?, ?> groupBacklog) {
        this._logger = LoggerFactory.getLogger(LOGGER_REPLICATION_BACKLOG + "." + config.getContainerName());
        this._groupBacklog = groupBacklog;
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
        T oldest = _redoFile.removeFirst();
        decreaseWeight(oldest);
        return oldest;
    }

    @Override
    public T getOldest() {
        return _redoFile.getFirst();
    }

    @Override
    public void add(T replicationPacket) {
        _redoFile.addLast(replicationPacket);
        increaseWeight(replicationPacket);
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
        } else {
            for (long i = 0; i < packetsCount; ++i) {
                removeOldest();
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
    }

    @Override
    public long getWeight() {
        return RedoLogCompactionUtil.calculateWeight(_weight, 0);
    }

    @Override
    public CompactionResult performCompaction(long from, long to) {
        final CompactionResult compactionResult = RedoLogCompactionUtil.compact(from, to, _redoFile.listIterator());
        //we remove only txn compacted packets since transient packets have the same weight when replaced as discarded
        this._weight -= compactionResult.getDeletedFromTxn();
        return compactionResult;
    }

    @Override
    public ReadOnlyIterator<T> readOnlyIterator(long fromKey) {
        return new ReadOnlyIteratorAdapter<>(iterator(fromKey));
    }

    public Iterator<T> iterator(long fromKey) {
        if (isEmpty()) return _redoFile.iterator();

        //fromKey = 25, filter: 25 <= k <= newest
        //oldest->[21, 22, 23, 24, 25, 26, 27, ..., 80]->newest
        long oldestKey = getOldest().getKey(); //25
        int fromIndex = (int) Math.max(0, (fromKey - oldestKey)); // index = (25 - 21) =4
        return _redoFile.subList(fromIndex, _redoFile.size()).iterator();
    }

    private void increaseWeight(T packet) {
        if (packet.isDiscardedPacket()) {
            _weight++;
        } else {
            _weight += packet.getWeight();
        }
    }

    private void decreaseWeight(T packet) {
        if (packet.isDiscardedPacket()) {
            _weight--;
        } else {
            _weight -= packet.getWeight();
        }
    }
}
