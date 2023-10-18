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
