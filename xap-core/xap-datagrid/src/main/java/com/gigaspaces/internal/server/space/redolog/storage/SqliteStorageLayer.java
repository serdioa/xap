package com.gigaspaces.internal.server.space.redolog.storage;

import com.gigaspaces.internal.cluster.node.impl.backlog.globalorder.GlobalOrderDiscardedReplicationPacket;
import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.server.space.redolog.DBSwapRedoLogFileConfig;
import com.gigaspaces.internal.utils.ByteUtils;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.start.SystemLocations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteConfig;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantLock;

public abstract class SqliteStorageLayer<T extends IReplicationOrderedPacket> {
    protected static final Logger logger = LoggerFactory.getLogger(Constants.LOGGER_REPLICATION_BACKLOG + ".sqlite");

    private static final String JDBC_DRIVER = "org.sqlite.JDBC";
    private static final String USER = "gs";
    private static final String PASS = "gigaspaces";
    protected static final String TABLE_NAME = "REDO_LOGS";

    private final Path path;
    private final String dbName;
    protected final Connection connection;
    private final ReentrantLock modifierLock = new ReentrantLock();
    private final DBSwapRedoLogFileConfig<T> config;
    protected final List<String> COLUMN_NAMES = Arrays.asList("redo_key", "type_name", "operation_type", "uuid", "packet_count", "packet_weight", "packet");
    protected final int REDO_KEY_COLUMN_INDEX = 1;
    protected final int TYPE_NAME_COLUMN_INDEX = 2;
    protected final int OPERATION_TYPE_COLUMN_INDEX = 3;
    protected final int UID_COLUMN_INDEX = 4;
    protected final int PACKET_COUNT_COLUMN_INDEX = 5;
    protected final int PACKET_WEIGHT_COLUMN_INDEX = 6;
    protected final int PACKET_COLUMN_INDEX = 7;


    protected SqliteStorageLayer(DBSwapRedoLogFileConfig<T> config) {
        this.config = config;
        this.path = SystemLocations.singleton().work("redo-log/" + config.getSpaceName());
        this.dbName = "sqlite_storage_redo_log_" + config.getFullMemberName();
        logger.info("Database path: " + path + ", file: " + dbName);
        if (!path.toFile().exists()) {
            if (!path.toFile().mkdirs()) {
                throw new StorageException("failed to mkdir " + path);
            }
        } else {
            deleteDataFile();
        }

        try {
            SQLiteConfig sqLiteConfig = new SQLiteConfig();
            String dbUrl = "jdbc:sqlite:" + path + "/" + dbName;
            connection = connectToDB(JDBC_DRIVER, dbUrl, USER, PASS, sqLiteConfig);
            logger.info("Successfully connected: " + dbUrl);
        } catch (ClassNotFoundException | SQLException e) {
            logger.error("Failed to initialize Sqlite RDBMS", e);
            throw new StorageException("failed to initialize internal sqlite RDBMS", e);
        }
        createTable();
    }

    private Connection connectToDB(String jdbcDriver, String dbUrl, String user, String password, SQLiteConfig sqLiteConfig) throws ClassNotFoundException, SQLException {
        Connection conn;
        Class.forName(jdbcDriver);
        sqLiteConfig.setPragma(SQLiteConfig.Pragma.JOURNAL_MODE, "wal");
        sqLiteConfig.setPragma(SQLiteConfig.Pragma.CACHE_SIZE, "5000");
        Properties properties = sqLiteConfig.toProperties();
        properties.setProperty("user", user);
        properties.setProperty("password", password);
        conn = DriverManager.getConnection(dbUrl, properties);
        return conn;
    }

    private void createTable() throws StorageException {
        String query = "CREATE TABLE " + TABLE_NAME +
                " ( 'redo_key' INTEGER, " +
                "'type_name' VARCHAR, " +
                "'operation_type' VARCHAR, " +
                "'uuid' VARCHAR, " +
                "'packet_count' INTEGER, " +
                "'packet_weight' INTEGER, " +
                "'packet' BLOB, " +
                "PRIMARY KEY (redo_key) );";
        try {
            executeCreateTable(query);
        } catch (SQLException e) {
            throw new StorageException("failed to create table " + TABLE_NAME, e);
        }
    }

    protected void executeCreateTable(String sqlQuery) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            try {
                modifierLock.lock();
                final int rowsAffected = statement.executeUpdate(sqlQuery);
                if (logger.isTraceEnabled()) {
                    logger.trace("executeCreateTable: {}, manipulated {} rows", sqlQuery, rowsAffected);
                }
            } finally {
                modifierLock.unlock();
            }
        }
    }

    protected long executeInsert(PreparedStatement statement) throws SQLException {
        try {
            modifierLock.lock();
            final long rowsAffected = Arrays.stream(statement.executeBatch()).sum();
            if (logger.isTraceEnabled()) {
                logger.trace("executeInsert manipulated {} rows", rowsAffected);
            }
            return rowsAffected;
        } finally {
            modifierLock.unlock();
        }
    }

    protected ResultSet executeQuery(String sqlQuery) throws SQLException {
        try {
            modifierLock.lock();
            if (logger.isTraceEnabled()) {
                logger.trace("executeQuery: {}", sqlQuery);
            }
            return connection.createStatement().executeQuery(sqlQuery);
        } finally {
            modifierLock.unlock();
        }

    }

    protected long executeDelete(String sqlQuery) throws SQLException {
        try {
            modifierLock.lock();
            final int rowsAffected = connection.createStatement().executeUpdate(sqlQuery);
            if (logger.isTraceEnabled()) {
                logger.trace("executeDelete: {}, manipulated {} rows", sqlQuery, rowsAffected);
            }
            return rowsAffected;
        } finally {
            modifierLock.unlock();
        }
    }

    protected byte[] packetToBytes(T packet) {
        try {
            return ByteUtils.objectToBytes(packet);
        } catch (IOException e) {
            if (logger.isWarnEnabled()) {
                logger.warn("Failed to serialize bytes from packet [" + packet + "]", e);
            }
        }
        return new byte[0];
    }

    protected T bytesToPacket(byte[] bytes) {
        try {
            return (T) ByteUtils.bytesToObject(bytes);
        } catch (IOException | ClassNotFoundException e) {
            if (logger.isWarnEnabled()) {
                logger.warn("Failed to deserialize packet from bytes", e);
            }
        }
        return null;
    }

    protected void dropTable() throws SQLException {
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

    public void deleteDataFile() {
        logger.info("Deleting database file {}", dbName);
        File folder = path.toFile();
        final File[] files = folder.listFiles((dir, name) -> name.matches(dbName + ".*"));
        if (files == null) {
            logger.error("Failed to delete db {} in path {}", dbName, path);
            throw new StorageException("Failed to delete db {} in path {}");
        }
        for (final File file : files) {
            if (!file.delete()) {
                logger.error("Can't remove " + file.getAbsolutePath());
            }
        }
        logger.info("Successfully deleted");
    }

    protected void logFailureInfo(ResultSet resultSet) throws SQLException {
        if (logger.isWarnEnabled()) {
            final long redoKey = resultSet.getLong(REDO_KEY_COLUMN_INDEX);
            final String typeName = resultSet.getString(TYPE_NAME_COLUMN_INDEX);
            final String operationType = resultSet.getString(OPERATION_TYPE_COLUMN_INDEX);
            final String uid = resultSet.getString(UID_COLUMN_INDEX);
            final int packetCount = resultSet.getInt(PACKET_COUNT_COLUMN_INDEX);
            logger.warn("Failed to read packet from storage: " +
                    "[redoKey=" + redoKey + ", typeName=" + typeName + ", operationType=" + operationType +
                    ", uid=" + uid + ", packetCount=" + packetCount + "], this packet was discarded");
        }
    }

    protected class RDBMSRedoLogIterator implements StorageReadOnlyIterator<T> {

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
                final T packet = bytesToPacket(resultSet.getBytes(PACKET_COLUMN_INDEX));
                if (packet == null) {
                    logFailureInfo(resultSet);
                    final long redoKey = resultSet.getLong(REDO_KEY_COLUMN_INDEX);
                    return (T) new GlobalOrderDiscardedReplicationPacket(redoKey);
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
