package com.gigaspaces.internal.server.space.redolog.storage;

import com.gigaspaces.internal.cluster.node.handlers.ITransactionInContext;
import com.gigaspaces.internal.cluster.node.impl.backlog.globalorder.GlobalOrderDiscardedReplicationPacket;
import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketEntryData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.operations.AbstractTransactionReplicationPacketData;
import com.gigaspaces.internal.server.space.redolog.DBSwapRedoLogFileConfig;
import com.gigaspaces.internal.server.space.redolog.storage.bytebuffer.IPacketStreamSerializer;
import com.gigaspaces.internal.server.space.redolog.storage.bytebuffer.SwapPacketStreamSerializer;
import com.gigaspaces.start.SystemLocations;
import com.gigaspaces.utils.FileUtils;
import com.gigaspaces.utils.RedologFlushNotifier;
import com.j_spaces.core.Constants;
import com.j_spaces.kernel.ClassLoaderHelper;
import com.j_spaces.kernel.SystemProperties;
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

import static com.gigaspaces.logger.Constants.LOGGER_REPLICATION_BACKLOG;

public abstract class SqliteStorageLayer<T extends IReplicationOrderedPacket> {
    protected final Logger logger;

    private static final String JDBC_DRIVER = "org.sqlite.JDBC";
    private static final String USER = "gs";
    private static final String PASS = "gigaspaces";
    protected static final String TABLE_NAME = "REDO_LOGS";

    private final Path path;
    private final String dbName;
    protected  Connection connection;
    private final ReentrantLock modifierLock = new ReentrantLock();
    protected final List<String> COLUMN_NAMES = Arrays.asList("redo_key", "type_name", "operation_type", "uuid", "packet_count", "packet_weight", "packet");
    protected final int REDO_KEY_COLUMN_INDEX = 1;
    protected final int TYPE_NAME_COLUMN_INDEX = 2;
    protected final int OPERATION_TYPE_COLUMN_INDEX = 3;
    protected final int UID_COLUMN_INDEX = 4;
    protected final int PACKET_COUNT_COLUMN_INDEX = 5;
    protected final int PACKET_WEIGHT_COLUMN_INDEX = 6;
    protected final int PACKET_COLUMN_INDEX = 7;
    private final DBPacketSerializer<T> packetSerializer;


    protected SqliteStorageLayer(DBSwapRedoLogFileConfig<T> config) {
        this.logger = LoggerFactory.getLogger(LOGGER_REPLICATION_BACKLOG + "." + config.getContainerName());
        this.path = SystemLocations.singleton().work("redo-log/" + config.getSpaceName());
        this.dbName = "sqlite_storage_redo_log_" + config.getContainerName();
        logger.info("Database path: " + path + ", file: " + dbName);
        if (!path.toFile().exists()) {
            if (!path.toFile().mkdirs()) {
                throw new StorageException("failed to mkdir " + path);
            }
        } else if (!config.shouldKeepDatabaseFile()) {
            boolean flushRedolog = SystemProperties.getBoolean(SystemProperties.REDOLOG_FLUSH_ON_SHUTDOWN, SystemProperties.REDOLOG_FLUSH_ON_SHUTDOWN_DEFAULT);
            if ( flushRedolog) {
                long redologSize = getStorageRedoLogSize();
                if (redologSize > 0) {
                    try {
                        String fullSpaceName = config.getContainerName() + ":" + config.getSpaceName();
                        logger.info("redolog for: " + config.getContainerName() + " about to copy to target");
                        Path target = FileUtils.copyRedologToTarget(config.getSpaceName(), fullSpaceName);
                        logger.info("redolog for: " + config.getContainerName() + " was copied to target");
                        FileUtils.notifyOnFlushRedologToStorage(fullSpaceName, config.getSpaceName(), redologSize, target);
                    } catch (Throwable t) {
                        logger.error("Fail to copy or notify redolog backup on startup :" + config.getContainerName(), t);
                    }
                }
            }
            deleteDataFile();
        }

        connectToDB();
        if (!config.shouldKeepDatabaseFile()){
            createTable();
        }

        final IPacketStreamSerializer<T> packetStreamSerializer = config.getPacketStreamSerializer();
        this.packetSerializer = new DBPacketSerializer<>(packetStreamSerializer == null
                ? new SwapPacketStreamSerializer<T>() //for tests - use default one
                : packetStreamSerializer);
    }


    private void connectToDB(){
        try {
            SQLiteConfig sqLiteConfig = new SQLiteConfig();
            if (Boolean.parseBoolean(System.getProperty(SystemProperties.SQLITE_ASYNC, "true")))
                sqLiteConfig.setSynchronous(SQLiteConfig.SynchronousMode.OFF);

            String dbUrl = "jdbc:sqlite:" + path + "/" + dbName;
            connection = connectToDB(JDBC_DRIVER, dbUrl, USER, PASS, sqLiteConfig);
            logger.info("Successfully connected: " + dbUrl);
        } catch (ClassNotFoundException | SQLException e) {
            logger.error("Failed to initialize Sqlite RDBMS", e);
            throw new StorageException("failed to initialize internal sqlite RDBMS", e);
        }
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

    protected long getStorageRedoLogSize() {
        boolean closeConnection = false;
        try {
            if (connection == null) {
                connectToDB();
                closeConnection = true;
            }
            String query = " SELECT COUNT(*) FROM " + TABLE_NAME;
            ResultSet rs = executeQuery(query);
            if (rs == null || !rs.next()) return 0;
            return rs.getLong(1);
        }
        catch (Throwable t){
            logger.error("Fail to get storage redolog size for: " +dbName);
            return  0;
        }
        finally {
            if (closeConnection) {
                try {
                    connection.close();
                }
                catch (SQLException e) {
                }
                connection = null;
            }
        }

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

    protected byte[] serializePacket(T packet) {
        try {
            return packetSerializer.serializePacket(packet);
        } catch (Exception e) {
            logSerializePacketFailureInfo(packet, e);
        }
        return new byte[0];
    }

    protected T deserializePacket(ResultSet resultSet) {
        try {
            final byte[] bytes = resultSet.getBytes(PACKET_COLUMN_INDEX);
            return packetSerializer.deserializePacket(bytes);
        } catch (Exception e) {
            logDeserializePacketFailureInfo(resultSet, e);
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

    private void logDeserializePacketFailureInfo(ResultSet resultSet, Exception e){
        if (logger.isWarnEnabled()) {
            String msg = "Failed to deserialize packet from bytes when reading packet from storage: ";
            try {
                final long redoKey = resultSet.getLong(REDO_KEY_COLUMN_INDEX);
                final String typeName = resultSet.getString(TYPE_NAME_COLUMN_INDEX);
                final String operationType = resultSet.getString(OPERATION_TYPE_COLUMN_INDEX);
                final String uid = resultSet.getString(UID_COLUMN_INDEX);
                final int packetCount = resultSet.getInt(PACKET_COUNT_COLUMN_INDEX);
                msg += "[redoKey=" + redoKey + ", typeName=" + typeName + ", operationType=" + operationType +
                        ", uid=" + uid + ", packetCount=" + packetCount + "], this packet was discarded.";
                logger.warn(msg, e);
            } catch (SQLException ignore) {
                logger.warn(msg, e);
            }
        }
    }

    private void logSerializePacketFailureInfo(T packet, Exception e) {
        if (logger.isWarnEnabled()) {
            String msg = "Failed to serialize bytes from packet [" + packet + "] when writing packet to storage: ";
            final long redoKey = packet.getKey();
            final String typeName = getPacketTypeName(packet);
            final String operationType = getPacketOperationTypeName(packet);
            final String uid = getPacketUID(packet);
            final int packetCount = getPacketCount(packet);
            msg += "[redoKey=" + redoKey + ", typeName=" + typeName + ", operationType=" + operationType +
                    ", uid=" + uid + ", packetCount=" + packetCount + "], this packet was discarded.";
            logger.warn(msg, e);
        }
    }


    protected int getPacketCount(T packet) {
        final IReplicationPacketData<?> data = packet.getData();
        final boolean isTnx = data != null && (!data.isSingleEntryData());
        if (isTnx) {
            return ((AbstractTransactionReplicationPacketData) data).getMetaData().getTransactionParticipantsCount();
        }
        return 1;
    }

    protected String getPacketTypeName(T packet) {
        final IReplicationPacketData<?> data = packet.getData();
        if (data != null) {
            if (data.isSingleEntryData()) {
                final IReplicationPacketEntryData singleEntryData = data.getSingleEntryData();
                if (singleEntryData != null) {
                    return singleEntryData.getTypeName();
                }
            }//else txn
        }
        return null;
    }

    protected String getPacketUID(T packet) {
        final IReplicationPacketData<?> data = packet.getData();
        if (data != null) {
            final boolean isTnx = !data.isSingleEntryData();
            if (isTnx) {
                ITransactionInContext txnPacketData = (ITransactionInContext) data;
                if (txnPacketData.getMetaData() == null) {
                    throw new IllegalArgumentException("Transaction packet without metadata to extract UID from: " + data);
                }
                return String.valueOf(txnPacketData.getMetaData().getTransactionUniqueId().getTransactionId());
            }
            final IReplicationPacketEntryData singleEntryData = data.getSingleEntryData();
            if (singleEntryData != null) {
                return singleEntryData.getUid();
            }
        }
        return null;
    }

    protected int getPacketWeight(T packet) {
        return packet.isDiscardedPacket() ? 1 : packet.getWeight();
    }

    protected String getPacketOperationTypeName(T packet) {
        final IReplicationPacketData<?> data = packet.getData();
        if (data != null) {
            if (data.isSingleEntryData()) {
                final IReplicationPacketEntryData singleEntryData = data.getSingleEntryData();
                if (singleEntryData != null) {
                    return singleEntryData.getOperationType().name();
                }
            }//else txn
        }
        return null;
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
                final T packet = deserializePacket(resultSet);
                if (packet == null) {
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
