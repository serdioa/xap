package com.gigaspaces.internal.server.space.redolog.storage;

import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketEntryData;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.gigaspaces.internal.server.space.redolog.RedoLogFileCompromisedException;
import com.gigaspaces.internal.server.space.redolog.storage.bytebuffer.WeightedBatch;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.start.SystemLocations;
import com.j_spaces.core.cluster.startup.CompactionResult;
import com.j_spaces.core.sadapter.SAException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteConfig;

import java.nio.file.Path;
import java.sql.*;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

public class RDBMSRedoLogFileStorage<T extends IReplicationOrderedPacket> implements IRedoLogFileStorage<T> {

    private static final String JDBC_DRIVER = "org.sqlite.JDBC";
    private static final String USER = "gs";
    private static final String PASS = "gigaspaces";
    private static final String SQLITE_EXPLAIN_PLAN_PREFIX = "EXPLAIN QUERY PLAN ";
    private static final String SQLITE_EXPLAIN_DETAILS_COLUMN = "detail";
    private static final String SQLITE_MATCH_INDEX_SCAN_REGEX = "USING INDEX";
    private static final String TABLE_NAME = "REDO_LOGS";

    private static Logger logger = LoggerFactory.getLogger(Constants.LOGGER_REPLICATION_BACKLOG + ".sqlite");
    private Path path;
    private String dbName;
    private String spaceName;
    private String fullMemberName;
    private Connection connection;
    private final ReentrantLock modifierLock = new ReentrantLock();
    private SpaceTypeManager typeManager;
    private Set<String> knownTypes = new HashSet<>();
    private long storageSize;


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
            executeUpdate(query);
        } catch (SQLException e) {
            throw new SAException("failed to create table " + TABLE_NAME, e);
        }
    }

    private int executeUpdate(String sqlQuery) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            try {
                modifierLock.lock();
                final int rowsAffected = statement.executeUpdate(sqlQuery);
                if (logger.isTraceEnabled()) {
                    logger.trace("executeUpdate: {}, manipulated {} rows", sqlQuery, rowsAffected);
                }
                return rowsAffected;
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
    public void appendBatch(List<T> replicationPackets) throws StorageException, StorageFullException {
        String query = "INSERT INTO " + TABLE_NAME + " (redo_key, type, operation_type, uuid, packet) VALUES ";
        Object[] values = new Object[replicationPackets.size() * 5];
        for (int i = 0; i < replicationPackets.size(); i++) {
            //TODO: validate what happen in tnx and if  getSingleEntryData == null (data.getSingleEntryData())
            final T replicationPacket = replicationPackets.get(i);
            final IReplicationPacketData<?> data = replicationPacket.getData();
            final IReplicationPacketEntryData iReplicationPacketEntryData = data != null ? data.getSingleEntryData() : null;
            values[(i * 5)] = replicationPacket.getKey();
            values[(i * 5) + 1] = "type?!?!?!!";
            values[(i * 5) + 2] = iReplicationPacketEntryData == null ? "null" : iReplicationPacketEntryData.getOperationType().name();
            values[(i * 5) + 3] = iReplicationPacketEntryData == null ? "null" : iReplicationPacketEntryData.getUid();
            values[(i * 5) + 4] = replicationPacket;
            query += "(?, ?, ?, ?, ?), ";
        }
        query = query.substring(0, query.length() - 2) + ";";
        try {
            executeUpdate(query, values);
        } catch (SQLException e) {
            throw new StorageException("failed to insert values to table " + TABLE_NAME, e);
        }
    }

    private void executeUpdate(String sqlQuery, Object[] values) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sqlQuery)) {
            for (int i = 0; i < values.length; i++) {
                setPropertyValue(true, statement, i, i + 1, values[i]);
            }
            try {
                modifierLock.lock();
                final int rowsAffected = statement.executeUpdate();
                if (sqlQuery.startsWith("INSERT INTO")) {
                    storageSize += rowsAffected;
                }
                if (sqlQuery.startsWith("DELETE")) { //todo use boolean
                    storageSize -= rowsAffected;
                }
                if (logger.isTraceEnabled()) {
                    logger.trace("executeUpdate: {}, values: {} , manipulated {} rows", sqlQuery, values, rowsAffected);
                }
            } finally {
                modifierLock.unlock();
            }
        }
    }

    public void setPropertyValue(boolean isUpdate, PreparedStatement statement, int columnIndex, int index,
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
                    statement.setObject(index, value);
//                    statement.setBlob(index, value); //TODO???
                    break;
            }
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
        String selectQuery = "SELECT * FROM " + TABLE_NAME + " ORDER BY redo_key LIMIT " + batchCapacity + ";";
        String deleteQuery = "DELETE FROM " + TABLE_NAME + " ORDER BY redo_key LIMIT " + batchCapacity + ";";
        try (ResultSet resultSet = executeQuery(selectQuery)) {
            while (resultSet.next()) {
                final Object object = resultSet.getObject(4);// TODO: or blobl?
                batch.addToBatch((T) object);
            }
        } catch (SQLException e) {
            throw new StorageException("failed to select rows table " + TABLE_NAME, e);
        }

        try {
            executeUpdate(deleteQuery, new Object[0]);
        } catch (SQLException e) {
            throw new StorageException("failed to delete values table " + TABLE_NAME, e);
        }
        return batch;
    }

    private ResultSet executeQuery(String sqlQuery) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            try {
                modifierLock.lock();
                return statement.executeQuery(sqlQuery);
            } finally {
                modifierLock.unlock();
            }
        }
    }

    @Override
    public void deleteOldestPackets(long packetsCount) throws StorageException {

    }

    @Override
    public StorageReadOnlyIterator<T> readOnlyIterator() throws StorageException {
        return null;
    }

    @Override
    public StorageReadOnlyIterator<T> readOnlyIterator(long fromIndex) throws StorageException {
        return null;
    }

    @Override
    public boolean isEmpty() throws StorageException {
        return storageSize == 0;
    }

    @Override
    public void validateIntegrity() throws RedoLogFileCompromisedException {

    }

    @Override
    public void close() {

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
        return 0;
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
