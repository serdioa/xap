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
package com.j_spaces.core.cache.mvcc;

import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.storage.*;
import com.gigaspaces.internal.utils.Textualizer;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.core.Constants;
import com.j_spaces.core.server.transaction.EntryXtnInfo;
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

    public MVCCEntryHolder createLogicallyDeletedDummyEntry(EntryXtnInfo entryXtnInfo) {
        ITransactionalEntryData ed = new FlatEntryData(
                new Object[0],
                null,
                getEntryData().getEntryTypeDesc(),
                0 /*versionID*/,
                Long.MAX_VALUE, /* expirationTime */
                entryXtnInfo);
        MVCCEntryHolder dummy = new MVCCEntryHolder(this.getServerTypeDesc(), this.getUID(), SystemTime.timeMillis(),
                this.isTransient(), ed);
        dummy.setLogicallyDeleted(true);
        return dummy;
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

    public void setVersion(int version){
        getEntryData().setVersion(version);
    }

    @Override
    public void toText(Textualizer textualizer) {
        textualizer.append("typeName", getClassName());
        textualizer.append("uid", getUID());
        textualizer.append("committedGeneration", getCommittedGeneration());
        textualizer.append("overrideGeneration", getOverrideGeneration());
        textualizer.append("isLogicallyDeleted", isLogicallyDeleted());
    }

}
