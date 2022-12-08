package com.gigaspaces.internal.server.storage;

import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.j_spaces.kernel.locks.IMVCCLockObject;


@com.gigaspaces.api.InternalApi
public class MVCCEntryHolder extends EntryHolder implements IMVCCLockObject {

    long createdGeneration = -1;
    long overrideGeneration = -1;
    int lockedObjectHashCode = -1;

    public MVCCEntryHolder(IServerTypeDesc typeDesc, String uid, long scn, boolean isTransient, ITransactionalEntryData entryData) {
        super(typeDesc, uid, scn, isTransient, entryData);
        initLockedObjectHashCodeAndSetGeneration();

    }

    protected MVCCEntryHolder(IEntryHolder other) {
        super(other);
        initLockedObjectHashCodeAndSetGeneration();
    }

    @Override
    public int getLockedObjectHashCode() {
        return lockedObjectHashCode;
    }

    private void initLockedObjectHashCodeAndSetGeneration() {
        final String uid = this.getUID();
        final int i = uid.lastIndexOf("#");
        if (i != -1) {
            final String generation = uid.substring(uid.lastIndexOf("#") + 1);
            this.createdGeneration = Long.parseLong(generation);
            final String uidPrefix =  uid.substring(0, uid.lastIndexOf("#"));
            this.lockedObjectHashCode = Math.abs(uidPrefix.hashCode());
        } else {
            this.lockedObjectHashCode = Math.abs(uid.hashCode());
        }
    }
}
