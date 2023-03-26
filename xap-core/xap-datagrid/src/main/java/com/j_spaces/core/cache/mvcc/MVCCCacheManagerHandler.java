package com.j_spaces.core.cache.mvcc;

import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.j_spaces.core.SpaceOperations;
import com.j_spaces.core.XtnEntry;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.IEntryCacheInfo;
import com.j_spaces.core.cache.XtnData;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.client.EntryAlreadyInSpaceException;
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
        IEntryHolder entryHolder = pEntry.getEntryHolder();
        if (oldEntry == null){
            oldEntry = new MVCCShellEntryCacheInfo(entryHolder, pEntry);
            entries.put(uid,oldEntry);
        } else if (isMvccEntryValidForWrite(oldEntry)) {
            oldEntry.setDirtyEntry(pEntry);
        } else{
            throw new EntryAlreadyInSpaceException(uid, entryHolder.getClassName());
        }
        //TODO: handle MVCCShellEntryCacheInfo's dirtyEntry when oldEntry != null
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

    public void handleNewMvccGeneration(Context context, IEntryHolder entry, XtnEntry xtnEntry) throws SAException {
        MVCCShellEntryCacheInfo mvccShellEntryCacheInfo = cacheManager.getMVCCShellEntryCacheInfoByUid(entry.getUID());
        MVCCEntryCacheInfo dirtyEntryCacheInfo = mvccShellEntryCacheInfo.getDirtyEntry();
        if (dirtyEntryCacheInfo != null) {
            MVCCEntryHolder dirtyEntryHolder = dirtyEntryCacheInfo.getMVCCEntryHolder();
            if (entry.getWriteLockOperation() == SpaceOperations.WRITE &&
                    entry.getWriteLockOwner() == xtnEntry && dirtyEntryHolder == entry) {
                mvccShellEntryCacheInfo.addEntryGeneration();
            }
            if (dirtyEntryHolder.getWriteLockOperation() == SpaceOperations.TAKE &&
                    dirtyEntryHolder.getWriteLockOwner() == xtnEntry &&
                    dirtyEntryHolder.isLogicallyDeleted() &&
                    dirtyEntryHolder.getCommittedGeneration() == ((MVCCEntryHolder) entry).getOverrideGeneration()){
                    disconnectMvccEntryFromXtn(context, dirtyEntryCacheInfo, xtnEntry, true);
                    mvccShellEntryCacheInfo.addEntryGeneration();
            }
        }
    }

    public boolean isMvccEntryValidForWrite(String uid) {
        return isMvccEntryValidForWrite(cacheManager.getMVCCShellEntryCacheInfoByUid(uid));
    }

    public boolean isMvccEntryValidForWrite(MVCCShellEntryCacheInfo shellEntryCacheInfo) {
        return shellEntryCacheInfo.getDirtyEntry() == null && shellEntryCacheInfo.isLogicallyDeletedOrEmpty();
    }
}
