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
package com.gigaspaces.internal.transport.mvcc;

import com.gigaspaces.internal.transport.IEntryPacket;
import com.j_spaces.core.cache.mvcc.MVCCEntryHolder;


/**
 * Implemented by entry packet(s) not in the same hierarchy as IEntryHolder interface.
 * Provides an interface for mvcc entries metadata replication.
 *
 * @author Davyd Savitskyi
 * @since 16.4.0
 */
public interface IMVCCEntryPacket extends IEntryPacket {

    default void applyMVCCEntryMetadata(MVCCEntryHolder eh) {
        throw new UnsupportedOperationException("Cannot apply MVCC entry metadata for this Entry Packet type: " +
                getClass().getTypeName());
    }

    default boolean isMVCCEntryMetadataApplied() {
        throw new UnsupportedOperationException("Cannot verify MVCC entry metadata for this Entry Packet type: " +
                getClass().getTypeName());
    }

    default MVCCEntryPacketMetadata getMVCCEntryMetadata() {
        throw new UnsupportedOperationException("Cannot retrieve MVCC entry metadata for this Entry Packet type: " +
                getClass().getTypeName());
    }

}
