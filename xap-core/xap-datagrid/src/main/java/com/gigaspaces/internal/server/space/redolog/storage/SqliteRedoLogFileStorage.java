package com.gigaspaces.internal.server.space.redolog.storage;

import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketEntryData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.operations.AbstractTransactionReplicationPacketData;
import com.gigaspaces.internal.server.space.redolog.DBSwapRedoLogFileConfig;
import com.gigaspaces.internal.server.space.redolog.RedoLogFileCompromisedException;
import com.gigaspaces.internal.server.space.redolog.storage.bytebuffer.WeightedBatch;
import com.j_spaces.core.cluster.startup.CompactionResult;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class SqliteRedoLogFileStorage<T extends IReplicationOrderedPacket> extends SqliteStorageLayer<T> implements INonBatchRedoLogFileStorage<T> {

    private final DBSwapRedoLogFileConfig<T> config;
    private long storageSize = 0;
    private long oldestKey = -1;

    public SqliteRedoLogFileStorage(DBSwapRedoLogFileConfig<T> config) {
        super(config);
        this.config = config;
    }

    @Override
    public void append(T replicationPacket) throws StorageException, StorageFullException {
        buffered.add(replicationPacket);
        if (buffered.size() == config.getFlushBufferPacketCount()) {
            appendBatch(buffered);
            buffered.clear();
        }
    }

    @Override
    public void appendBatch(List<T> replicationPackets) throws StorageException, StorageFullException {
        boolean isEmpty = isEmpty();
        long lastOldKey = oldestKey;
        String query = "INSERT INTO " +
                TABLE_NAME + " (" + String.join(", ", COLUMN_NAMES) + ") " +
                "VALUES (?, ?, ?, ?, ?, ?);";
        try (Statement tnx = connection.createStatement()) {
            tnx.execute("BEGIN TRANSACTION;");
            try (PreparedStatement statement = connection.prepareStatement(query)) {
                for (final T replicationPacket : replicationPackets) {
                    if (isEmpty) {
                        oldestKey = replicationPacket.getKey();
                        isEmpty = false;
                    }
                    statement.setLong(1, replicationPacket.getKey());
                    statement.setString(2, getPacketTypeName(replicationPacket));
                    statement.setString(3, getPacketOperationTypeName(replicationPacket));
                    statement.setString(4, getPacketUID(replicationPacket));
                    statement.setInt(5, getPacketCount(replicationPacket));
                    statement.setBytes(6, packetToBytes(replicationPacket));
                    statement.addBatch();
                }
                storageSize += executeInsert(statement);
            } catch (SQLException e) {
                oldestKey = lastOldKey;
                tnx.execute("ROLLBACK;");
                throw new StorageException("failed to insert redo-log keys " + replicationPackets.get(0).getKey() + "-" +
                        replicationPackets.get(replicationPackets.size() - 1).getKey() + " to table " + TABLE_NAME, e);
            }
            tnx.execute("COMMIT;");
        } catch (SQLException e) {
            oldestKey = lastOldKey;
            throw new StorageException("failed to commit transaction" + TABLE_NAME, e);
        }
    }

    private int getPacketCount(T packet) {
        final IReplicationPacketData<?> data = packet.getData();
        final boolean isTnx = data != null && (!data.isSingleEntryData());
        if (isTnx) {
            return ((AbstractTransactionReplicationPacketData) data).getMetaData().getTransactionParticipantsCount();
        }
        return 1;
    }

    private String getPacketTypeName(T packet) {
        final IReplicationPacketData<?> data = packet.getData();
        if (data != null) {
            final IReplicationPacketEntryData singleEntryData = data.getSingleEntryData();
            if (singleEntryData != null) {
                return singleEntryData.getTypeName();
            }
        }
        return null;
    }

    private String getPacketUID(T packet) {
        final IReplicationPacketData<?> data = packet.getData();
        if (data != null) {
            final boolean isTnx = !data.isSingleEntryData();
            if (isTnx) {
                return String.valueOf(((AbstractTransactionReplicationPacketData) data).getTransaction().id);
            }
            final IReplicationPacketEntryData singleEntryData = data.getSingleEntryData();
            if (singleEntryData != null) {
                return singleEntryData.getUid();
            }
        }
        return null;
    }

    private String getPacketOperationTypeName(T packet) {
        final IReplicationPacketData<?> data = packet.getData();
        if (data != null) {
            final IReplicationPacketEntryData singleEntryData = data.getSingleEntryData();
            if (singleEntryData != null) {
                return singleEntryData.getOperationType().name();
            }
        }
        return null;
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
                final T packet = bytesToPacket(resultSet.getBytes(PACKET_COLUMN_INDEX));
                if (packet == null) {
                    if (logger.isWarnEnabled()) {
                        logger.warn("Failed to read packet from RDBMS, packetId=" + resultSet.getLong(REDO_KEY_COLUMN_INDEX));
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
                final T packet = bytesToPacket(resultSet.getBytes(PACKET_COLUMN_INDEX));
                if (packet == null) {
                    if (logger.isWarnEnabled()) {
                        logger.warn("Failed to read packet from RDBMS, packetId=" + resultSet.getLong(REDO_KEY_COLUMN_INDEX));
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
