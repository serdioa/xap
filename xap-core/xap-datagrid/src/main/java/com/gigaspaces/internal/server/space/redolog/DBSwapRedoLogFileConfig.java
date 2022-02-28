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
import com.gigaspaces.internal.server.space.redolog.storage.IRedoLogFileStorage;

/**
 * Configures a {@link DBSwapRedoLogFileConfig}
 *
 * @author eitany
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class DBSwapRedoLogFileConfig<T extends IReplicationOrderedPacket> {
    private final int _memoryPacketCapacity;
    private final int _diskPacketCapacity;
    private final IRedoLogFileStorage<T> _redoLogFileStorage;
    private final int _flushPacketSize;

    public DBSwapRedoLogFileConfig(int memoryPacketCapacity,
                                   int diskPacketCapacity, IRedoLogFileStorage<T> redoLogFileStorage) {
        this._diskPacketCapacity = diskPacketCapacity;
        this._memoryPacketCapacity = memoryPacketCapacity;
        this._flushPacketSize = memoryPacketCapacity / 4;
        this._redoLogFileStorage = redoLogFileStorage;

    }

    public int getMemoryPacketCapacity() {
        return _memoryPacketCapacity;
    }

    public IRedoLogFileStorage<T> getRedoLogFileStorage() {
        return _redoLogFileStorage;
    }

    public int getDiskPacketCapacity() {
        return _diskPacketCapacity;
    }

    public int getFlushPacketSize() {
        return _flushPacketSize;
    }
}