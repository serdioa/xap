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

package com.gigaspaces.internal.server.space.redolog;

import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketDataProducer;
import com.j_spaces.core.cluster.SwapBacklogConfig;

/**
 * Configures a {@link DBSwapRedoLogFileConfig}
 *
 * @since 16.2
 */
@com.gigaspaces.api.InternalApi
public class DBSwapRedoLogFileConfig<T extends IReplicationOrderedPacket> {
    private final String spaceName; //e.g. mySpace
    private final String containerName; //e.g. mySpace_container1
    private final int memoryPacketCapacity;
    private int flushBufferPacketCount;
    private IReplicationPacketDataProducer<?> dataProducer;
    private boolean keepDatabaseFile;

    public DBSwapRedoLogFileConfig(String spaceName, String containerName, int memoryPacketCapacity) {
        this.spaceName = spaceName;
        this.containerName = containerName;
        this.memoryPacketCapacity = memoryPacketCapacity;
        this.flushBufferPacketCount = SwapBacklogConfig.SQLITE_FLUSH_BUFFER_PACKETS_COUNT_DEFAULT;
    }

    public String getSpaceName() {
        return spaceName;
    }

    public String getContainerName() {
        return containerName;
    }

    public int getMemoryPacketCapacity() {
        return memoryPacketCapacity;
    }

    public void setFlushBufferPacketCount(int flushBufferPacketCount) {
        this.flushBufferPacketCount = flushBufferPacketCount;
    }

    public IReplicationPacketDataProducer<?> getDataProducer() {
        return dataProducer;
    }

    public void setDataProducer(IReplicationPacketDataProducer<?> _dataProducer) {
        this.dataProducer = _dataProducer;
    }

    public int getFlushBufferPacketCount() {
        return flushBufferPacketCount;
    }

    public void setKeepDatabaseFile(boolean keepDatabaseFile){
        this.keepDatabaseFile = keepDatabaseFile;
    }

    public boolean shouldKeepDatabaseFile() {
        return keepDatabaseFile;
    }

    @Override
    public String toString() {
        return "DBSwapRedoLogFileConfig{" +
                "spaceName='" + spaceName + '\'' +
                ", containerName='" + containerName + '\'' +
                ", memoryPacketCapacity=" + memoryPacketCapacity +
                ", flushBufferPacketCount=" + flushBufferPacketCount +
                ", keepDatabaseFile=" + keepDatabaseFile +
                '}';
    }
}