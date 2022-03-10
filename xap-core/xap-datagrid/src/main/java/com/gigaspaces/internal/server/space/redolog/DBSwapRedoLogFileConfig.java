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
import com.j_spaces.core.cluster.SwapBacklogConfig;

/**
 * Configures a {@link DBSwapRedoLogFileConfig}
 *
 * @since 16.2
 */
@com.gigaspaces.api.InternalApi
public class DBSwapRedoLogFileConfig<T extends IReplicationOrderedPacket> {
    private final String spaceName;
    private final String fullMemberName;
    private final int memoryPacketCapacity;
    private int flushBufferPacketCount;

    public DBSwapRedoLogFileConfig(String spaceName, String fullMemberName, int memoryPacketCapacity) {
        this.spaceName = spaceName;
        this.fullMemberName = fullMemberName;
        this.memoryPacketCapacity = memoryPacketCapacity;
        this.flushBufferPacketCount = SwapBacklogConfig.SQLITE_FLUSH_BUFFER_PACKETS_COUNT_DEFAULT;
    }

    public String getSpaceName() {
        return spaceName;
    }

    public String getFullMemberName() {
        return fullMemberName;
    }

    public int getMemoryPacketCapacity() {
        return memoryPacketCapacity;
    }

    public void setFlushBufferPacketCount(int flushBufferPacketCount) {
        this.flushBufferPacketCount = flushBufferPacketCount;
    }

    public int getFlushBufferPacketCount() {
        return flushBufferPacketCount;
    }

    @Override
    public String toString() {
        return "DBSwapRedoLogFileConfig{" +
                "spaceName='" + spaceName + '\'' +
                ", fullMemberName='" + fullMemberName + '\'' +
                ", memoryPacketCapacity=" + memoryPacketCapacity +
                ", flushBufferPacketCount=" + flushBufferPacketCount +
                '}';
    }
}