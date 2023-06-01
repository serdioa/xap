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

package org.openspaces.itest.persistency.cassandra.helper;

import com.gigaspaces.logger.GSLogConfigLoader;
import org.apache.cassandra.cql.jdbc.CassandraDataSource;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.openspaces.itest.persistency.cassandra.helper.config.CassandraTestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.rmi.Remote;
import java.sql.Connection;
import java.sql.SQLException;


public class EmbeddedCassandra implements IEmbeddedCassandra, Remote {
    public static final String RPC_PORT_PROP = "cassandra.rpc_port";
    public static final int DEFAULT_RPC_PORT = 9160;

    private static final String CQL_VERSION = "3.0.0";
    private static final String USERNAME = "default";
    private static final String PASSWORD = "password";
    private static final String SYSTEM_KEYSPACE_NAME = "system";
    private static final String LOCALHOST = "localhost";

    private final Logger _logger = LoggerFactory.getLogger(getClass().getName());

    private final int _rpcPort;

    public EmbeddedCassandra() {
        GSLogConfigLoader.getLoader();
        cleanup();
        _rpcPort = Integer.getInteger(RPC_PORT_PROP, DEFAULT_RPC_PORT);
        _logger.info("Starting Embedded Cassandra with keyspace ");
        try {
            EmbeddedCassandraServerHelper.startEmbeddedCassandra();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        _logger.info("Started Embedded Cassandra");
    }

    @Override
    public void createKeySpace(String keySpace) {
        try {
            createKeySpaceImpl(keySpace);
        } catch (Exception e) {
            _logger.error("Could not create keyspace " + keySpace + " for embedded Cassandra", e);
        }
    }

    @Override
    public void dropKeySpace(String keySpace) {
        try {
            dropKeySpaceImpl(keySpace);
        } catch (Exception e) {
            _logger.error("Could not drop keyspace " + keySpace + " for embedded Cassandra", e);
        }
    }

    @Override
    public void destroy() {
        EmbeddedCassandraServerHelper.stopEmbeddedCassandra();
    }

    private void cleanup() {
        try {
            EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
            CassandraTestUtils.deleteFileOrDirectory(new File("target/cassandra"));
        } catch (IOException e) {
            _logger.warn("Failed deleting cassandra directory", e);
        }
    }

    private void createKeySpaceImpl(String keySpace) throws SQLException {
        executeUpdate("CREATE KEYSPACE " + keySpace + " " +
                "WITH strategy_class = 'SimpleStrategy' " +
                "AND strategy_options:replication_factor = 1");
    }

    private void dropKeySpaceImpl(String keySpace) throws SQLException {
        executeUpdate("DROP KEYSPACE " + keySpace);
    }

    private void executeUpdate(String statement) throws SQLException {
        //String host, int port, String keyspace, String user, String password, String version, String consistency
        CassandraDataSource ds = new CassandraDataSource(LOCALHOST,
                _rpcPort,
                SYSTEM_KEYSPACE_NAME,
                USERNAME,
                PASSWORD,
                CQL_VERSION,
                null);
        Connection conn = ds.getConnection();
        conn.createStatement().executeUpdate(statement);
        conn.close();
    }

}
