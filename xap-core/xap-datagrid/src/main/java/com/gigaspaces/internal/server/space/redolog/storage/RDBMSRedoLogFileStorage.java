package com.gigaspaces.internal.server.space.redolog.storage;

import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketEntryData;
import com.gigaspaces.internal.server.space.redolog.RedoLogFileCompromisedException;
import com.gigaspaces.internal.server.space.redolog.storage.bytebuffer.WeightedBatch;
import com.gigaspaces.internal.utils.ByteUtils;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.start.SystemLocations;
import com.j_spaces.core.cluster.startup.CompactionResult;
import com.j_spaces.core.sadapter.SAException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteConfig;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantLock;

public class RDBMSRedoLogFileStorage<T extends IReplicationOrderedPacket> implements INonBatchRedoLogFileStorage<T> {

    private static final String JDBC_DRIVER = "org.sqlite.JDBC";
    private static final String USER = "gs";
    private static final String PASS = "gigaspaces";
    private static final String TABLE_NAME = "REDO_LOGS";

    private static final Logger logger = LoggerFactory.getLogger(Constants.LOGGER_REPLICATION_BACKLOG + ".sqlite");
    private final Path path;
    private final String dbName;
    private final String spaceName;
    private final String fullMemberName;
    private final Connection connection;
    private final ReentrantLock modifierLock = new ReentrantLock();
    private long storageSize;
    private long oldestKey = 1;
    private final ArrayList<T> buffered = new ArrayList<>(20_000);


    public RDBMSRedoLogFileStorage(String spaceName, String fullMemberName) throws SAException {
        this.spaceName = spaceName;
        this.fullMemberName = fullMemberName;
        this.path = SystemLocations.singleton().work("redo-log/" + spaceName); // todo: maybe in temp
        this.dbName = "sqlite_storage_redo_log_" + fullMemberName;

        if (!path.toFile().exists()) {
            if (!path.toFile().mkdirs()) {
                throw new SAException("failed to mkdir " + path);
            }
        }

        try {
            SQLiteConfig config = new SQLiteConfig();
            String dbUrl = "jdbc:sqlite:" + path + "/" + dbName;
            connection = connectToDB(JDBC_DRIVER, dbUrl, USER, PASS, config);
            logger.info("Successfully created connection {} db {} in path {}", connection, dbName, path);
        } catch (ClassNotFoundException | SQLException e) {
            logger.error("Failed to initialize Sqlite RDBMS", e);
            throw new SAException("failed to initialize internal sqlite RDBMS", e);
        }
        createTable();
    }

    private void createTable() throws SAException {
        String query = "CREATE TABLE " + TABLE_NAME +
                " ( 'redo_key' INTEGER, 'type' VARCHAR, 'operation_type' VARCHAR, 'uuid' VARCHAR, 'packet' BLOB," +
                " PRIMARY KEY (redo_key) );";
        try {
            executeCreateTable(query);
        } catch (SQLException e) {
            throw new SAException("failed to create table " + TABLE_NAME, e);
        }
    }

    private void executeCreateTable(String sqlQuery) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            try {
                modifierLock.lock();
                final int rowsAffected = statement.executeUpdate(sqlQuery);
                if (logger.isTraceEnabled()) {
                    logger.trace("executeUpdate: {}, manipulated {} rows", sqlQuery, rowsAffected);
                }
            } finally {
                modifierLock.unlock();
            }
        }
    }

    private Connection connectToDB(String jdbcDriver, String dbUrl, String user, String password, SQLiteConfig config) throws ClassNotFoundException, SQLException {
        Connection conn;
        Class.forName(jdbcDriver);
        config.setPragma(SQLiteConfig.Pragma.JOURNAL_MODE, "wal");
        config.setPragma(SQLiteConfig.Pragma.CACHE_SIZE, "5000");
        Properties properties = config.toProperties();
        properties.setProperty("user", user);
        properties.setProperty("password", password);
        conn = DriverManager.getConnection(dbUrl, properties);
        return conn;
    }

    @Override
    public void append(T replicationPacket) throws StorageException, StorageFullException {
        //TODO implement without array
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
            executeInsert(query, values);
        } catch (SQLException e) {
            oldestKey = 1;
            throw new StorageException("failed to insert values to table " + TABLE_NAME, e);
        }
    }

    private void executeInsert(String sqlQuery, Object[] values) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sqlQuery)) {
            for (int i = 0; i < values.length; i++) {
                setPropertyValue(true, statement, i, i + 1, values[i]);
            }
            try {
                modifierLock.lock();
                final int rowsAffected = statement.executeUpdate();
                storageSize += rowsAffected;
                if (logger.isTraceEnabled()) {
                    logger.trace("executeUpdate: {}, values: {} , manipulated {} rows", sqlQuery, values, rowsAffected);
                }
            } finally {
                modifierLock.unlock();
            }
        }
    }

    private void setPropertyValue(boolean isUpdate, PreparedStatement statement, int columnIndex, int index,
                                  Object value) throws SQLException {
        if (value == null) {
            if (isUpdate) {
                statement.setObject(index, null);
            } else {
                statement.setString(index, "Null");
            }
        } else {
            switch (columnIndex % 5) {
                case 0: //redo_key
                    statement.setLong(index, (Long) value);
                    break;
                case 1: //type
                    statement.setString(index, (String) value);
                    break;
                case 2: //operation_type
                    statement.setString(index, (String) value);
                    break;
                case 3: //uuid
                    statement.setString(index, (String) value);
                    break;
                case 4: //packet
                    statement.setBytes(index, packetToBytes((T) value));
                    break;
            }
        }
    }

    private byte[] packetToBytes(T packet) {
        try {
            return ByteUtils.objectToBytes(packet);
        } catch (IOException e) {
            if (logger.isWarnEnabled()) {
                logger.warn("Failed to create bytes from packet [" + packet + "]", e);
            }
        }
        return new byte[0];
    }

    private T bytesToPacket(byte[] bytes) {
        try {
            return (T) ByteUtils.bytesToObject(bytes);
        } catch (IOException | ClassNotFoundException e) {
            if (logger.isWarnEnabled()) {
                logger.warn("Failed to create packet from bytes", e);
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
            executeDelete(deleteQuery);
        } catch (SQLException e) {
            throw new StorageException("failed to delete values table " + TABLE_NAME, e);
        }
        return batch;
    }

    private ResultSet executeQuery(String sqlQuery) throws SQLException {
        try {
            modifierLock.lock();
            return connection.createStatement().executeQuery(sqlQuery);
        } finally {
            modifierLock.unlock();
        }

    }

    private void executeDelete(String sqlQuery) throws SQLException {
        try {
            modifierLock.lock();
            final int rowsAffected = connection.createStatement().executeUpdate(sqlQuery);
            storageSize -= rowsAffected;
            oldestKey += rowsAffected;
        } finally {
            modifierLock.unlock();
        }
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
            oldestKey = 1;

        } catch (SQLException e) {
            throw new IllegalStateException(new StorageException("Fail to close storage", e));
        }
    }

    private void dropTable() throws SQLException {
        String query = "DROP TABLE '" + TABLE_NAME + "';";
        try (Statement statement = connection.createStatement()) {
            try {
                modifierLock.lock();
                final int rowsAffected = statement.executeUpdate(query);
                if (logger.isTraceEnabled()) {
                    logger.trace("executeUpdate: {}, manipulated {} rows", query, rowsAffected);
                }
            } finally {
                modifierLock.unlock();

            }
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
        return null;
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

    private class RDBMSRedoLogIterator implements StorageReadOnlyIterator<T> {

        private final ResultSet resultSet;

        public RDBMSRedoLogIterator(ResultSet resultSet) {
            this.resultSet = resultSet;
        }

        @Override
        public boolean hasNext() throws StorageException {
            try {
                return resultSet.next();
            } catch (SQLException e) {
                throw new StorageException(e);
            }
        }

        @Override
        public T next() throws StorageException {
            try {
                final T packet = bytesToPacket(resultSet.getBytes(5));
                if (packet == null) {
                    if (logger.isWarnEnabled()) {
                        logger.warn("Failed to read packet from RDBMS, packetId=" + resultSet.getLong(1));
                    }
                }
                return packet;
            } catch (SQLException e) {
                throw new StorageException(e);
            }
        }

        @Override
        public void close() throws StorageException {
            try {
                resultSet.close();
            } catch (SQLException e) {
                throw new StorageException(e);
            }
        }
    }
}
