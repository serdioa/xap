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

import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.j_spaces.core.XtnEntry;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.IEntryCacheInfo;
import com.j_spaces.core.cache.XtnData;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.sadapter.SAException;

import java.util.concurrent.ConcurrentMap;

public class MVCCCacheManagerHandler {


    private final CacheManager cacheManager;

    public MVCCCacheManagerHandler(CacheManager cacheManager) {
        this.cacheManager = cacheManager;
    }

    public IEntryCacheInfo insertMvccEntryToCache(MVCCEntryCacheInfo pEntry, ConcurrentMap<String, IEntryCacheInfo> entries) {
        String uid = pEntry.getUID();
        MVCCShellEntryCacheInfo oldEntry = (MVCCShellEntryCacheInfo) entries.get(uid);
        if (oldEntry == null){
            oldEntry = new MVCCShellEntryCacheInfo(pEntry.getEntryHolder(), pEntry);
            entries.put(uid,oldEntry);
        }
        //TODO: handle MVCCShellEntryCacheInfo's dirtyEntry when oldEntry != null
        return null;
    }

    public void disconnectPEntryFromXtn(Context context, MVCCEntryCacheInfo pEntry, XtnEntry xtnEntry, boolean xtnEnd)
            throws SAException {
        if (pEntry == null)
            return; //already disconnected (writelock replaced)
        XtnData pXtn = xtnEntry.getXtnData();

        if (!xtnEnd)
            cacheManager.removeLockedEntry(pXtn, pEntry);

        IEntryHolder entryHolder = pEntry.getEntryHolder(cacheManager);

        if ((entryHolder.getWriteLockTransaction() == null) ||
                !entryHolder.getWriteLockTransaction().equals(pXtn.getXtn())) {
            entryHolder.removeReadLockOwner(pXtn.getXtnEntry());
        }

        if ((entryHolder.getWriteLockTransaction() != null) &&
                entryHolder.getWriteLockTransaction().equals(pXtn.getXtn())) {
            entryHolder.resetWriteLockOwner();
        }

        if (xtnEntry == entryHolder.getXidOriginated())
            entryHolder.resetXidOriginated();

        entryHolder.setMaybeUnderXtn(entryHolder.anyReadLockXtn() || entryHolder.getWriteLockTransaction() != null);

        // unpin entry if relevant
        if (!entryHolder.isMaybeUnderXtn()) {
            if (!entryHolder.hasWaitingFor()) {
                entryHolder.resetEntryXtnInfo();
                if (pEntry.isPinned() && xtnEnd)
                    cacheManager.unpinIfNeeded(context, entryHolder, null /*template*/, pEntry);
            }
        }
    }

}
