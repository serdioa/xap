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
