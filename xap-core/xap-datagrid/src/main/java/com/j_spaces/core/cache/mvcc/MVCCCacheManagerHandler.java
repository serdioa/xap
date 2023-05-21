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

import com.gigaspaces.internal.server.space.mvcc.MVCCIllegalStateException;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.j_spaces.core.SpaceOperations;
import com.j_spaces.core.XtnEntry;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.IEntryCacheInfo;
import com.j_spaces.core.cache.XtnData;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.sadapter.SAException;
import com.j_spaces.core.server.transaction.EntryXtnInfo;
import com.j_spaces.kernel.IStoredList;

import java.util.concurrent.ConcurrentMap;

public class MVCCCacheManagerHandler {


    private final CacheManager cacheManager;

    public MVCCCacheManagerHandler(CacheManager cacheManager) {
        this.cacheManager = cacheManager;
    }

    public IEntryCacheInfo insertMvccEntryToCache(MVCCEntryCacheInfo pEntry, ConcurrentMap<String, IEntryCacheInfo> entries) {
        String uid = pEntry.getUID();
        MVCCShellEntryCacheInfo oldEntry = (MVCCShellEntryCacheInfo) entries.get(uid);
        IEntryHolder entryHolder = pEntry.getEntryHolder();
        if (oldEntry == null){
            oldEntry = new MVCCShellEntryCacheInfo(entryHolder, pEntry);
            entries.put(uid,oldEntry);
        } else if (isMvccEntryValidForWrite(oldEntry)) {
            oldEntry.setDirtyEntryCacheInfo(pEntry);
        } else{
            return oldEntry;
        }
        return null;
    }

    public void disconnectMvccEntryFromXtn(Context context, MVCCEntryCacheInfo pEntry, XtnEntry xtnEntry, boolean xtnEnd)
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

    public void handleNewMvccGeneration(Context context, MVCCEntryHolder entry, XtnEntry xtnEntry) throws SAException {
        MVCCShellEntryCacheInfo mvccShellEntryCacheInfo = cacheManager.getMVCCShellEntryCacheInfoByUid(entry.getUID());
        MVCCEntryCacheInfo latestGenerationCacheInfo = mvccShellEntryCacheInfo.getLatestGenerationCacheInfo();
        if (latestGenerationCacheInfo == null) {
            throw new MVCCIllegalStateException("latest generation doesn't exist during commit for transaction: " +
                    xtnEntry.getXtnData().getXtn() + " with generation state: " + xtnEntry.getMVCCGenerationsState());
        } else {
            //is take operation (for mvcc we set writeLockOwner to be EXCLUSIVE_READ_LOCK see MVCCSpaceEngineHandler.preCommit)
            MVCCEntryHolder latestGenerationEntryHolder = latestGenerationCacheInfo.getEntryHolder();
            if (latestGenerationEntryHolder.getWriteLockOperation() == SpaceOperations.TAKE &&
                    latestGenerationEntryHolder.getCommittedGeneration() == entry.getOverrideGeneration() &&
                    latestGenerationEntryHolder.isLogicallyDeleted()) {
                    disconnectMvccEntryFromXtn(context, latestGenerationCacheInfo, xtnEntry, true);
            }
        }
    }

    public boolean isMvccEntryValidForWrite(String uid) {
        return isMvccEntryValidForWrite(cacheManager.getMVCCShellEntryCacheInfoByUid(uid));
    }

    public boolean isMvccEntryValidForWrite(MVCCShellEntryCacheInfo shellEntryCacheInfo) {
        return shellEntryCacheInfo.getDirtyEntryCacheInfo() == null && shellEntryCacheInfo.isLogicallyDeletedOrEmpty();
    }

    public void insertMvccEntryRefs(MVCCEntryCacheInfo pEntry, XtnData pXtn) {
        IStoredList<IEntryCacheInfo> newEntries = pXtn.getNewEntries(true);
        if (!newEntries.contains(pEntry)) {
            newEntries.add(pEntry);
        }
    }

    public void createLogicallyDeletedEntry(MVCCEntryHolder entryHolder) {
        MVCCShellEntryCacheInfo mvccShellEntryCacheInfo = cacheManager.getMVCCShellEntryCacheInfoByUid(entryHolder.getUID());
        EntryXtnInfo entryXtnInfo = entryHolder.getTxnEntryData().copyTxnInfo(true, false);
        entryXtnInfo.setWriteLockOperation(SpaceOperations.TAKE);
        MVCCEntryHolder dummyEntry = entryHolder.createLogicallyDeletedDummyEntry(entryXtnInfo);
        dummyEntry.setMaybeUnderXtn(true);
        mvccShellEntryCacheInfo.setDirtyEntryCacheInfo(new MVCCEntryCacheInfo(dummyEntry, 2));
    }
}
