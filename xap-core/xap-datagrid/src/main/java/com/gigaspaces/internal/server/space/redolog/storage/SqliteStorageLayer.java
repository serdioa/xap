package com.gigaspaces.internal.server.space.redolog.storage;

import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.utils.ByteUtils;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.start.SystemLocations;
import com.j_spaces.core.sadapter.SAException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteConfig;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.*;
import java.util.ArrayList;
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
    private final String spaceName;
    private final String fullMemberName;
    protected final Connection connection;
    private final ReentrantLock modifierLock = new ReentrantLock();
    protected final ArrayList<T> buffered = new ArrayList<>(20_000); //TODO: validate configuration

    protected SqliteStorageLayer(String spaceName, String fullMemberName) throws SAException {
        this.spaceName = spaceName;
        this.fullMemberName = fullMemberName;
        this.path = SystemLocations.singleton().work("redo-log/" + spaceName); // todo: maybe in temp
        this.dbName = "sqlite_storage_redo_log_" + fullMemberName;

        if (!path.toFile().exists()) {
            if (!path.toFile().mkdirs()) {
                throw new SAException("failed to mkdir " + path);
            }
        } else {
            deleteDataFile();
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

    protected long executeInsert(String sqlQuery, Object[] values) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sqlQuery)) {
            for (int i = 0; i < values.length; i++) {
                setPropertyValue(true, statement, i, i + 1, values[i]);
            }
            try {
                modifierLock.lock();
                final int rowsAffected = statement.executeUpdate();
                if (logger.isTraceEnabled()) {
                    logger.trace("executeInsert: {}, values: {} , manipulated {} rows", sqlQuery, values, rowsAffected);
                }
                return rowsAffected;
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
                logger.warn("Failed to create bytes from packet [" + packet + "]", e);
            }
        }
        return new byte[0];
    }

    protected T bytesToPacket(byte[] bytes) {
        try {
            return (T) ByteUtils.bytesToObject(bytes);
        } catch (IOException | ClassNotFoundException e) {
            if (logger.isWarnEnabled()) {
                logger.warn("Failed to create packet from bytes", e);
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

    public void deleteDataFile() throws SAException {
        logger.info("Trying to delete db file {}", dbName);
        File folder = path.toFile();
        final File[] files = folder.listFiles((dir, name) -> name.matches(dbName + ".*"));
        if (files == null) {
            logger.error("Failed to delete db {} in path {}", dbName, path);
            throw new SAException("Failed to delete db {} in path {}");
        }
        for (final File file : files) {
            if (!file.delete()) {
                logger.error("Can't remove " + file.getAbsolutePath());
            }
        }
        logger.info("Successfully deleted db {} in path {}", dbName, path);
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
