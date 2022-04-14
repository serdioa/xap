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
package com.gigaspaces.internal.server.space.redolog.storage;

import com.gigaspaces.internal.cluster.node.impl.backlog.globalorder.GlobalOrderDiscardedReplicationPacket;
import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.server.space.redolog.DBSwapRedoLogFileConfig;
import com.gigaspaces.internal.server.space.redolog.RedoLogFileCompromisedException;
import com.gigaspaces.internal.server.space.redolog.storage.bytebuffer.WeightedBatch;
import com.j_spaces.core.cluster.startup.CompactionResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.NoSuchElementException;

import static com.gigaspaces.logger.Constants.LOGGER_REPLICATION_BACKLOG;

public class SqliteRedoLogFileStorage<T extends IReplicationOrderedPacket> extends SqliteStorageLayer<T> implements IRedoLogFileStorage<T> {

    private final Logger logger;
    private long storageSize = 0;
    private long storageWeight = 0;
    private long oldestKey = -1;

    public SqliteRedoLogFileStorage(DBSwapRedoLogFileConfig<T> config) {
        super(config);
        this.logger = LoggerFactory.getLogger(LOGGER_REPLICATION_BACKLOG + "." + config.getFullMemberName());
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
                    final int packetWeight = getPacketWeight(replicationPacket);
                    statement.setLong(REDO_KEY_COLUMN_INDEX, replicationPacket.getKey());
                    statement.setString(TYPE_NAME_COLUMN_INDEX, getPacketTypeName(replicationPacket));
                    statement.setString(OPERATION_TYPE_COLUMN_INDEX, getPacketOperationTypeName(replicationPacket));
                    statement.setString(UID_COLUMN_INDEX, getPacketUID(replicationPacket));
                    statement.setInt(PACKET_COUNT_COLUMN_INDEX, getPacketCount(replicationPacket));
                    statement.setInt(PACKET_WEIGHT_COLUMN_INDEX, packetWeight);
                    statement.setBytes(PACKET_COLUMN_INDEX, serializePacket(replicationPacket));
                    statement.addBatch();
                    batchWeight += packetWeight;
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
            if (logger.isTraceEnabled()) {
                logger.trace("flushing #" + replicationPackets.size() + " packets to underlying storage");
            }
        } catch (SQLException e) {
            storageSize = lastStorageSize;
            storageWeight = lastStorageWeight;
            oldestKey = lastOldKey;
            throw new StorageException("failed to commit transaction for table: " + TABLE_NAME, e);
        }
    }

    private long updateOldestKey(long rowsAffected) {
        return (storageSize == 0) ? oldestKey + rowsAffected - 1 : oldestKey + rowsAffected;
    }

    @Override
    public long size() throws StorageException {
        return storageSize;
    }


    @Override
    public WeightedBatch<T> removeFirstBatch(int batchCapacity, long lastCompactionRangeEndKey) throws
            StorageException {
        throw new UnsupportedOperationException();
    }


    @Override
    public void deleteOldestPackets(long packetsCount) throws StorageException {
        String whereClause = " WHERE redo_key >= " + oldestKey + " AND redo_key < " + (oldestKey + packetsCount) + ";";
        String selectSumQuery = "SELECT SUM(packet_weight) FROM " + TABLE_NAME + whereClause;
        String deleteQuery = "DELETE FROM " + TABLE_NAME + whereClause;
        int removedWeight = 0;
        try (ResultSet resultSet = executeQuery(selectSumQuery)) {
            while (resultSet.next()) {
                removedWeight = resultSet.getInt(1);
            }
        } catch (SQLException e) {
            throw new StorageException("failed to select rows table " + TABLE_NAME, e);
        }

        try {
            long rowsAffected = executeDelete(deleteQuery);
            storageSize -= rowsAffected;
            storageWeight -= removedWeight;
            oldestKey = updateOldestKey(rowsAffected);
        } catch (SQLException e) {
            throw new StorageException("failed to delete values table " + TABLE_NAME, e);
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
                final T packet = deserializePacket(resultSet);
                if (packet == null) {
                    final long redoKey = resultSet.getLong(REDO_KEY_COLUMN_INDEX);
                    return (T) new GlobalOrderDiscardedReplicationPacket(redoKey);
                }
                return packet;
            }
        } catch (SQLException e) {
            throw new StorageException("Fail to get oldest", e);
        }
        throw new IllegalStateException("Reached empty result set with the following query=" + query);
    }

    @Override
    public T removeOldest() throws StorageException {
        T oldest = getOldest();
        String query = "DELETE FROM " + TABLE_NAME + " WHERE redo_key = " + oldestKey + ";";
        try {
            long rowsAffected = executeDelete(query);
            storageSize -= rowsAffected;
            storageWeight -= getPacketWeight(oldest);
            oldestKey = updateOldestKey(rowsAffected);
        } catch (SQLException e) {
            throw new StorageException("failed to delete using query: " + query, e);
        }
        return oldest;
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
        //todo: remove from interface!!!!!!
        throw new UnsupportedOperationException();
    }

    @Override
    public CompactionResult performCompaction(long from, long to) {
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
