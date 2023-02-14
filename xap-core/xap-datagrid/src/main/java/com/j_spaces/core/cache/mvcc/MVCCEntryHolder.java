package com.j_spaces.core.cache.mvcc;

import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.storage.EntryHolder;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.server.storage.ITransactionalEntryData;
import com.j_spaces.core.Constants;
import com.j_spaces.kernel.locks.IMVCCLockObject;


@com.gigaspaces.api.InternalApi
public class MVCCEntryHolder extends EntryHolder implements IMVCCLockObject {

    private volatile long committedGeneration = -1;
    private volatile long overrideGeneration = -1;

    public MVCCEntryHolder(IServerTypeDesc typeDesc, String uid, long scn, boolean isTransient, ITransactionalEntryData entryData) {
        super(typeDesc, uid, scn, isTransient, entryData);

    }

    protected MVCCEntryHolder(MVCCEntryHolder other) {
        super(other);
        this.committedGeneration = other.getCommittedGeneration();
        this.overrideGeneration = other.getOverrideGeneration();
    }

    @Override
    public IEntryHolder createCopy() {
        return new MVCCEntryHolder(this);
    }

    @Override
    public int getLockedObjectHashCode() {
        return Math.abs(getUID().hashCode());
    }

    public long getCommittedGeneration() {
        return committedGeneration;
    }

    public void setCommittedGeneration(long committedGeneration) {
        this.committedGeneration = committedGeneration;
    }

    public long getOverrideGeneration() {
        return overrideGeneration;
    }

    public void setOverrideGeneration(long overrideGeneration) {
        this.overrideGeneration = overrideGeneration;
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
}
