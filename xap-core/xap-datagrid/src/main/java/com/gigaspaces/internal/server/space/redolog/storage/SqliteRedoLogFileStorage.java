package com.gigaspaces.internal.server.space.redolog.storage;

import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketEntryData;
import com.gigaspaces.internal.server.space.redolog.RedoLogFileCompromisedException;
import com.gigaspaces.internal.server.space.redolog.storage.bytebuffer.WeightedBatch;
import com.j_spaces.core.cluster.startup.CompactionResult;
import com.j_spaces.core.sadapter.SAException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class SqliteRedoLogFileStorage<T extends IReplicationOrderedPacket> extends SqliteStorageLayer<T> implements INonBatchRedoLogFileStorage<T> {

    private long storageSize = 0;
    private long oldestKey = -1;

    public SqliteRedoLogFileStorage(String spaceName, String fullMemberName) throws SAException {
        super(spaceName, fullMemberName);
    }

    @Override
    public void append(T replicationPacket) throws StorageException, StorageFullException {
        buffered.add(replicationPacket);
        if (buffered.size() == 20_000) {
            appendBatch(buffered);
            buffered.clear();
        }
    }

    @Override
    public void appendBatch(List<T> replicationPackets) throws StorageException, StorageFullException {
        boolean isEmpty = isEmpty();
        String query = "INSERT INTO " + TABLE_NAME + " (redo_key, type, operation_type, uuid, packet) VALUES ";
        Object[] values = new Object[replicationPackets.size() * 5];
        for (int i = 0; i < replicationPackets.size(); i++) {
            //TODO: validate what happen in tnx and if  getSingleEntryData == null (data.getSingleEntryData())
            final T replicationPacket = replicationPackets.get(i);
            final IReplicationPacketData<?> data = replicationPacket.getData();
            final IReplicationPacketEntryData iReplicationPacketEntryData = data != null ? data.getSingleEntryData() : null;
            if (isEmpty) {
                oldestKey = replicationPacket.getKey();
                isEmpty = false;
            }
            values[(i * 5)] = replicationPacket.getKey();
            values[(i * 5) + 1] = "type?!?!?!!";
            values[(i * 5) + 2] = iReplicationPacketEntryData == null ? "null" : iReplicationPacketEntryData.getOperationType().name();
            values[(i * 5) + 3] = iReplicationPacketEntryData == null ? "null" : iReplicationPacketEntryData.getUid();
            values[(i * 5) + 4] = replicationPacket;
            query += "(?, ?, ?, ?, ?), ";
        }
        query = query.substring(0, query.length() - 2) + ";";
        try {
            long rowsAffected = executeInsert(query, values);
            storageSize += rowsAffected;
        } catch (SQLException e) {
            oldestKey = -1;
            throw new StorageException("failed to insert values to table " + TABLE_NAME, e);
        }
    }

    @Override
    public long size() throws StorageException {
        return storageSize;
    }

    @Override
    public WeightedBatch<T> removeFirstBatch(int batchCapacity, long lastCompactionRangeEndKey) throws
            StorageException {
        WeightedBatch<T> batch = new WeightedBatch<T>(lastCompactionRangeEndKey);

        String whereClause = " WHERE redo_key >= " + oldestKey + " AND redo_key < " + (oldestKey + batchCapacity) + ";";
        String selectQuery = "SELECT * FROM " + TABLE_NAME + whereClause;
        String deleteQuery = "DELETE FROM " + TABLE_NAME + whereClause;
        if (isEmpty() && !buffered.isEmpty()) {
            appendBatch(buffered);
            buffered.clear();
        }
        try (ResultSet resultSet = executeQuery(selectQuery)) {
            while (resultSet.next()) {
                final T packet = bytesToPacket(resultSet.getBytes(5));
                if (packet == null) {
                    if (logger.isWarnEnabled()) {
                        logger.warn("Failed to read packet from RDBMS, packetId=" + resultSet.getLong(1));
                    }
                } else {
                    batch.addToBatch(packet);
                }
            }
        } catch (SQLException e) {
            throw new StorageException("failed to select rows table " + TABLE_NAME, e);
        }

        try {
            long rowsAffected = executeDelete(deleteQuery);
            storageSize -= rowsAffected;
            oldestKey += rowsAffected;
        } catch (SQLException e) {
            throw new StorageException("failed to delete values table " + TABLE_NAME, e);
        }
        return batch;
    }


    @Override
    public void deleteOldestPackets(long packetsCount) throws StorageException {
        removeFirstBatch((int) packetsCount, -1);
    }

    @Override
    public StorageReadOnlyIterator<T> readOnlyIterator() throws StorageException {
        String query = "SELECT * FROM " + TABLE_NAME + " ORDER BY redo_key;";
        if (isEmpty() && !buffered.isEmpty()) {
            appendBatch(buffered);
            buffered.clear();
        }
        try (final ResultSet resultSet = executeQuery(query)) {
            return new RDBMSRedoLogIterator(resultSet);
        } catch (SQLException e) {
            throw new StorageException("Failed to create readOnlyIterator", e);
        }
    }

    @Override
    public StorageReadOnlyIterator<T> readOnlyIterator(long fromIndex) throws StorageException {
        String query = "SELECT * FROM " + TABLE_NAME + " WHERE redo_key >= " + fromIndex + " ORDER BY redo_key;";
        if (isEmpty() && !buffered.isEmpty()) {
            appendBatch(buffered);
            buffered.clear();
        }
        try {
            final ResultSet resultSet = executeQuery(query);
            return new RDBMSRedoLogIterator(resultSet);
        } catch (SQLException e) {
            throw new StorageException("Failed to create readOnlyIterator", e);
        }
    }

    @Override
    public boolean isEmpty() throws StorageException {
        return storageSize == 0;
    }

    @Override
    public void validateIntegrity() throws RedoLogFileCompromisedException {

    }

    @Override
    public T getOldest() throws StorageException {
        //TODO: considering using cache to keep more
        String query = "SELECT * FROM " + TABLE_NAME + " WHERE redo_key = " + oldestKey + ";";
        if (isEmpty() && !buffered.isEmpty()) {
            appendBatch(buffered);
            buffered.clear();
        }
        try (final ResultSet resultSet = executeQuery(query)) {
            if (resultSet.next()) {
                final T packet = bytesToPacket(resultSet.getBytes(5));
                if (packet == null) {
                    if (logger.isWarnEnabled()) {
                        logger.warn("Failed to read packet from RDBMS, packetId=" + resultSet.getLong(1));
                    }
                } else {
                    return packet;
                }
            }
        } catch (SQLException e) {
            throw new StorageException("Fail to get oldest", e);
        }
        return null;
    }

    @Override
    public void close() {
        try {
            dropTable();
            if (connection != null) {
                connection.close();
            }
            storageSize = 0;
            oldestKey = -1;

        } catch (SQLException e) {
            throw new IllegalStateException(new StorageException("Fail to close storage", e));
        }
    }

    @Override
    public long getWeight() {
        return 0;
    }

    @Override
    public long getDiscardedPacketsCount() {
        return 0;
    }

    @Override
    public CompactionResult performCompaction(long from, long to) {
        //TODO: @sagiv currently we not support it
        throw new UnsupportedOperationException("performCompaction is not support in RDBMS layer");
    }

    @Override
    public long getCacheWeight() {
        return 0;
    }

    @Override
    public long getSpaceUsed() {
        return 0;
    }

    @Override
    public long getExternalPacketsCount() {
        return storageSize;
    }

    @Override
    public long getMemoryPacketsCount() {
        return 0;
    }

    @Override
    public long getMemoryPacketsWeight() {
        return 0;
    }

    @Override
    public long getExternalStoragePacketsWeight() {
        return 0;
    }
}
