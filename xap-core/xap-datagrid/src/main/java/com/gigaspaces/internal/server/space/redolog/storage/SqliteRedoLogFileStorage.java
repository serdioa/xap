package com.gigaspaces.internal.server.space.redolog.storage;

import com.gigaspaces.internal.cluster.node.impl.backlog.globalorder.GlobalOrderDiscardedReplicationPacket;
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
import java.util.NoSuchElementException;

public class SqliteRedoLogFileStorage<T extends IReplicationOrderedPacket> extends SqliteStorageLayer<T> implements IRedoLogFileStorage<T> {

    private final DBSwapRedoLogFileConfig<T> config;
    private long storageSize = 0;
    private long storageWeight = 0;
    private long oldestKey = -1;

    public SqliteRedoLogFileStorage(DBSwapRedoLogFileConfig<T> config) {
        super(config);
        this.config = config;
    }

    @Override
    public void appendBatch(List<T> replicationPackets) throws StorageException {
        long lastOldKey = oldestKey;
        long batchWeight = 0;
        long lastStorageSize = storageSize;
        long lastStorageWeight = storageWeight;
        String query = "INSERT INTO " +
                TABLE_NAME + " (" + String.join(", ", COLUMN_NAMES) + ") " +
                "VALUES (?, ?, ?, ?, ?, ?, ?);";
        try (Statement tnx = connection.createStatement()) {
            tnx.execute("BEGIN TRANSACTION;");
            try (PreparedStatement statement = connection.prepareStatement(query)) {
                for (final T replicationPacket : replicationPackets) {
                    if (lastOldKey == oldestKey && storageSize == 0) {
                        oldestKey = replicationPacket.getKey();
                    }
                    statement.setLong(1, replicationPacket.getKey());
                    statement.setString(2, getPacketTypeName(replicationPacket));
                    statement.setString(3, getPacketOperationTypeName(replicationPacket));
                    statement.setString(4, getPacketUID(replicationPacket));
                    statement.setInt(5, getPacketCount(replicationPacket));
                    statement.setInt(6, replicationPacket.getWeight());
                    statement.setBytes(7, packetToBytes(replicationPacket));
                    statement.addBatch();
                    batchWeight += replicationPacket.getWeight();
                }
                long rowsAffected = executeInsert(statement);
                storageWeight += batchWeight;
                storageSize += rowsAffected;
            } catch (SQLException e) {
                oldestKey = lastOldKey;
                tnx.execute("ROLLBACK;"); //todo: we lost all the packets in the list
                throw new StorageException("failed to insert redo-log keys " + replicationPackets.get(0).getKey() + "-" +
                        replicationPackets.get(replicationPackets.size() - 1).getKey() + " to table " + TABLE_NAME, e);
            }
            tnx.execute("COMMIT;");
        } catch (SQLException e) {
            storageSize = lastStorageSize;
            storageWeight = lastStorageWeight;
            oldestKey = lastOldKey;
            throw new StorageException("failed to commit transaction for table: " + TABLE_NAME, e);
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

        String selectQuery = "SELECT * FROM\n" +
                "(\n" +
                "    SELECT \n" +
                "    *, \n" +
                "    SUM(packet_weight) OVER(ORDER BY redo_key ROWS \n" +
                "       BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS Total\n" +
                "FROM " + TABLE_NAME + "\n" +
                ") T\n" +
                "WHERE T.Total <= " + batchCapacity;
        int removedWeight = 0;
        long newestKeyRemoved = -1;
        try (ResultSet resultSet = executeQuery(selectQuery)) {
            while (resultSet.next()) {
                final T packet = bytesToPacket(resultSet.getBytes(PACKET_COLUMN_INDEX));
                if (packet == null) {
                    logFailureInfo(resultSet);
                    final long redoKey = resultSet.getLong(REDO_KEY_COLUMN_INDEX);
                    batch.addToBatch((T) new GlobalOrderDiscardedReplicationPacket(redoKey));
                } else {
                    batch.addToBatch(packet);
                }
                removedWeight += resultSet.getInt(PACKET_WEIGHT_COLUMN_INDEX);
                newestKeyRemoved = resultSet.getLong(REDO_KEY_COLUMN_INDEX);
            }
        } catch (SQLException e) {
            throw new StorageException("failed to select rows table " + TABLE_NAME, e);
        }

        try {
            String deleteQuery = "DELETE FROM " + TABLE_NAME + " WHERE redo_key >= " + oldestKey + " AND redo_key <= " + (newestKeyRemoved) + ";";
            long rowsAffected = executeDelete(deleteQuery);
            storageSize -= rowsAffected;
            storageWeight -= removedWeight;
            oldestKey += rowsAffected;
        } catch (SQLException e) {
            throw new StorageException("failed to delete values table " + TABLE_NAME, e);
        }
        return batch;
    }


    @Override
    public void deleteOldestPackets(long packetsCount) throws StorageException {
        String whereClause = " WHERE redo_key >= " + oldestKey + " AND redo_key < " + (oldestKey + packetsCount) + ";";
        String selectSumQuery = "SELECT SUM(packet_weight) FROM " + TABLE_NAME + whereClause;
        String deleteQuery = "DELETE FROM " + TABLE_NAME + whereClause;
        int removedWeight = 0;
        try (ResultSet resultSet = executeQuery(selectSumQuery)) {
            while (resultSet.next()) {
                removedWeight = resultSet.getInt(1);;
            }
        } catch (SQLException e) {
            throw new StorageException("failed to select rows table " + TABLE_NAME, e);
        }

        try {
            long rowsAffected = executeDelete(deleteQuery);
            storageSize -= rowsAffected;
            storageWeight -= removedWeight;
            oldestKey += rowsAffected;
        } catch (SQLException e) {
            throw new StorageException("failed to delete values table " + TABLE_NAME, e);
        }
    }

    @Override
    public StorageReadOnlyIterator<T> readOnlyIterator() throws StorageException {
        String query = "SELECT * FROM " + TABLE_NAME + " ORDER BY redo_key;";
        try (final ResultSet resultSet = executeQuery(query)) {
            return new RDBMSRedoLogIterator(resultSet);
        } catch (SQLException e) {
            throw new StorageException("Failed to create readOnlyIterator", e);
        }
    }

    @Override
    public StorageReadOnlyIterator<T> readOnlyIterator(long fromIndex) throws StorageException {
        String query = "SELECT * FROM " + TABLE_NAME + " WHERE redo_key >= " + fromIndex + " ORDER BY redo_key;";
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
        if (isEmpty()) {
            throw new NoSuchElementException();
        }
        String query = "SELECT * FROM " + TABLE_NAME + " WHERE redo_key = " + oldestKey + ";";
        try (final ResultSet resultSet = executeQuery(query)) {
            if (resultSet.next()) {
                final T packet = bytesToPacket(resultSet.getBytes(PACKET_COLUMN_INDEX));
                if (packet == null) {
                    logFailureInfo(resultSet);
                    final long redoKey = resultSet.getLong(REDO_KEY_COLUMN_INDEX);
                    return (T) new GlobalOrderDiscardedReplicationPacket(redoKey);
                }
                return packet;
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
        return storageWeight;
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
        return storageWeight;
    }
}
