package com.gigaspaces.internal.transport.mvcc;

import com.gigaspaces.internal.server.space.redolog.storage.bytebuffer.ISwapExternalizable;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.serialization.SmartExternalizable;
import com.j_spaces.core.Constants;
import com.j_spaces.core.EntrySerializationException;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * MVCC entry's metadata. Passing as a part of entry packet for recovery replication.
 *
 * @author Davyd Savitskyi
 * @since 16.4.0
 */
@com.gigaspaces.api.InternalApi
public class MVCCEntryPacketMetadata implements SmartExternalizable, ISwapExternalizable {

    private static final long serialVersionUID = -744373855246627533L;

    private long _committedGeneration;
    private long _overrideGeneration;
    private short _flags;

    public MVCCEntryPacketMetadata() {
    }

    public long getCommittedGeneration() {
        return _committedGeneration;
    }

    public void setCommittedGeneration(long committedGeneration) {
        this._committedGeneration = committedGeneration;
    }

    public long getOverrideGeneration() {
        return _overrideGeneration;
    }

    public void setOverrideGeneration(long overrideGeneration) {
        this._overrideGeneration = overrideGeneration;
    }

    public boolean isOverridingAnother() {
        return getFlag(Constants.SpaceItem.IS_OVERRIDING_ANOTHER);
    }

    public void setOverridingAnother(boolean overridingAnother) {
        setFlag(Constants.SpaceItem.IS_OVERRIDING_ANOTHER, overridingAnother);
    }

    public boolean isLogicallyDeleted() {
        return getFlag(Constants.SpaceItem.IS_LOGICALLY_DELETED);
    }

    public void setLogicallyDeleted(boolean logicallyDeleted) {
        setFlag(Constants.SpaceItem.IS_LOGICALLY_DELETED, logicallyDeleted);
    }

    private void setFlag(byte mask, boolean value) {
        if (value) {
            _flags |= mask; // set bit to 1
        } else {
            _flags &= ~mask; // set bit to 0
        }
    }

    private boolean getFlag(byte mask) {
        return (_flags & mask) > 0; // bit is 1
    }


    @Override
    public void writeToSwap(ObjectOutput out) throws IOException {
        serialize(out);
    }

    @Override
    public void readFromSwap(ObjectInput in) throws IOException, ClassNotFoundException {
        deserialize(in);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        serialize(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        deserialize(in);
    }

    private void serialize(ObjectOutput out) {
        try {
            if (LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v16_4_0)) {
                out.writeShort(_flags);
                out.writeLong(_committedGeneration);
                out.writeLong(_overrideGeneration);
            }
        } catch (EntrySerializationException e) {
            throw e;
        } catch (Exception e) {
            throw new EntrySerializationException("Failed to serialize Entry " + getClass().getTypeName(), e);
        }
    }

    private void deserialize(ObjectInput in) {
        try {
            if (LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v16_4_0)) {
                _flags = in.readShort();
                _committedGeneration = in.readLong();
                _overrideGeneration = in.readLong();
            }
        } catch (EntrySerializationException e) {
            throw e;
        } catch (Exception e) {
            throw new EntrySerializationException("Failed to deserialize Entry " + getClass().getTypeName(), e);
        }
    }

    @Override
    public String toString() {
        return "MVCCEntryPacketMetadata{" +
                "_committedGeneration=" + _committedGeneration +
                ", _overrideGeneration=" + _overrideGeneration +
                ", deleted=" + isLogicallyDeleted() +
                ", override_another=" + isOverridingAnother() +
                '}';
    }
}
