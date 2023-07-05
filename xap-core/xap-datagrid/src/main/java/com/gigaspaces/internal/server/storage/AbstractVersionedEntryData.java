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
