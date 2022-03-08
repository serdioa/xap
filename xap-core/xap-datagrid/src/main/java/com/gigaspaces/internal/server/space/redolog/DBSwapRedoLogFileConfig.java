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

/**
 * Configures a {@link DBSwapRedoLogFileConfig}
 *
 * @author eitany
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class DBSwapRedoLogFileConfig<T extends IReplicationOrderedPacket> {
    private final String _spaceName;
    private final String _fullMemberName;
    private final int _memoryPacketCapacity;
    private final int _diskPacketCapacity;
    private int _flushBufferPacketCount;
    private String _onRedoLogCapacityExceededString;

    public DBSwapRedoLogFileConfig(String spaceName, String fullMemberName, int memoryPacketCapacity, int diskPacketCapacity) {
        this._spaceName = spaceName;
        this._fullMemberName = fullMemberName;
        this._diskPacketCapacity = diskPacketCapacity;
        this._memoryPacketCapacity = memoryPacketCapacity;
        this._flushBufferPacketCount = 1_500;
    }

    public String getSpaceName() {
        return _spaceName;
    }

    public String getFullMemberName() {
        return _fullMemberName;
    }

    public int getMemoryPacketCapacity() {
        return _memoryPacketCapacity;
    }

    public int getDiskPacketCapacity() {
        return _diskPacketCapacity;
    }

    public void setFlushBufferPacketCount(int flushBufferPacketCount) {
        this._flushBufferPacketCount = flushBufferPacketCount;
    }

    public int getFlushBufferPacketCount() {
        return _flushBufferPacketCount;
    }
}