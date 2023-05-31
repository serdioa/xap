package com.gigaspaces.internal.server.storage;

import com.gigaspaces.internal.metadata.EntryTypeDesc;
import com.j_spaces.core.server.transaction.EntryXtnInfo;

public abstract class AbstractVersionedEntryData implements ITransactionalEntryData{

    protected final EntryTypeDesc _entryTypeDesc;
    protected int _versionID;
    protected final long _expirationTime;
    protected final EntryXtnInfo _entryTxnInfo;


    public AbstractVersionedEntryData(EntryTypeDesc entryTypeDesc, int version, long expirationTime, EntryXtnInfo entryXtnInfo) {
        this._entryTypeDesc = entryTypeDesc;
        this._versionID = version;
        this._expirationTime = expirationTime;
        this._entryTxnInfo = entryXtnInfo;
    }

    @Override
    public void setVersion(int _versionID) {
        this._versionID = _versionID;
    }

}
