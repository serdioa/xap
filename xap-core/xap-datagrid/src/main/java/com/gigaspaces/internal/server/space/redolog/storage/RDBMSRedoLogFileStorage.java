package com.gigaspaces.internal.server.space.redolog.storage;

import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.gigaspaces.internal.server.space.redolog.RedoLogFileCompromisedException;
import com.gigaspaces.internal.server.space.redolog.storage.bytebuffer.WeightedBatch;
import com.gigaspaces.start.SystemLocations;
import com.j_spaces.core.cluster.startup.CompactionResult;
import com.j_spaces.core.sadapter.SAException;
import org.slf4j.Logger;
import org.sqlite.SQLiteConfig;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
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

    private Logger logger;
    private Path path;
    private String dbName;
    private String spaceName;
    private String fullMemberName;
    private Connection connection;
    private final ReentrantLock modifierLock = new ReentrantLock();
    private SpaceTypeManager typeManager;
    private Set<String> knownTypes = new HashSet<>();


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
        String query = "CREATE TABLE " + TABLE_NAME + " (PRIMARY KEY (redo_key), 'redo_key' INTEGER, 'type' VARCHAR, 'operation_type' INTEGER, 'uuid' VARCHAR, 'packet' BLOB);";
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

    }

    @Override
    public long size() throws StorageException {
        return 0;
    }

    @Override
    public WeightedBatch<T> removeFirstBatch(int batchCapacity, long lastCompactionRangeEndKey) throws StorageException {
        return null;
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
        return false;
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
