package com.gigaspaces.transport;

import com.gigaspaces.internal.utils.GsEnv;

public class PocSettings {
    public static final boolean enabled = GsEnv.propertyBoolean("com.gs.nio.enabled").get(false);
    public static final String host = GsEnv.property("com.gs.nio.host").get("localhost");
    public static final int port = GsEnv.propertyInt("com.gs.nio.port").get(8080);
    public static final boolean directBuffers = GsEnv.propertyBoolean("com.gs.nio.direct-buffers").get(false);
    //public static final boolean customMarshal = GsEnv.propertyBoolean("com.gs.nio.custom-marshal").get(true);
    public static final boolean clientConnectionPoolDynamic = GsEnv.propertyBoolean("com.gs.nio.client.connection-pool.dynamic").get(false);
    public static final int clientConnectionPoolSize = GsEnv.propertyInt("com.gs.nio.client.connection-pool.size").get(4);
    public static final int serverReaderPoolSize = GsEnv.propertyInt("com.gs.nio.server.reader-pool-size").get(4);

    public static String dump() {
        return "host: " + host + ", " +
                "port: " + port + ", " +
                "direct-buffers: " + directBuffers + ", " +
                //"custom-marshal: " + customMarshal + ", " +
                "client.connection-pool.dynamic: " + clientConnectionPoolDynamic + ", " +
                "client.connection-pool.size: " + clientConnectionPoolSize + ", " +
                "server.reader-pool-size: " + serverReaderPoolSize;
    }
}
