package com.j_spaces.core.cache.mvcc;

import com.gigaspaces.internal.server.space.mvcc.MVCCIllegalStateException;
import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.j_spaces.core.SpaceOperations;
import com.j_spaces.core.XtnEntry;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.IEntryCacheInfo;
import com.j_spaces.core.cache.TypeData;
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
        IEntryHolder entryHolder = pEntry.getEntryHolder();
        if (oldEntry == null){
            oldEntry = new MVCCShellEntryCacheInfo(entryHolder, pEntry);
            entries.put(uid,oldEntry);
        } else if (oldEntry.getDirtyEntryCacheInfo() == null) {
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
        } else{
            //is take operation (for mvcc we set writeLockOwner to be EXCLUSIVE_READ_LOCK see MVCCSpaceEngineHandler.preCommit)
            MVCCEntryHolder latestGenerationEntryHolder = latestGenerationCacheInfo.getEntryHolder();
            if (latestGenerationEntryHolder.getWriteLockOperation() == SpaceOperations.READ &&
                    latestGenerationEntryHolder.getCommittedGeneration() == entry.getOverrideGeneration() &&
                    latestGenerationEntryHolder.isLogicallyDeleted()){
                    disconnectMvccEntryFromXtn(context, latestGenerationCacheInfo, xtnEntry, true);
            }
            if (latestGenerationEntryHolder.getWriteLockOperation() == SpaceOperations.UPDATE &&
                    latestGenerationEntryHolder.getCommittedGeneration() == entry.getOverrideGeneration()){
                disconnectMvccEntryFromXtn(context, latestGenerationCacheInfo, xtnEntry, true);
            }
        }
    }

    public boolean isMvccEntryValidForWriteOnly(String uid) {
        return isMvccEntryValidForWriteOnly(cacheManager.getMVCCShellEntryCacheInfoByUid(uid));
    }

    public boolean isMvccEntryValidForWriteOnly(MVCCShellEntryCacheInfo shellEntryCacheInfo) {
        return shellEntryCacheInfo.getDirtyEntryCacheInfo() == null && shellEntryCacheInfo.isLogicallyDeletedOrEmpty();
    }

    public void createUpdateMvccEntryPendingGeneration(Context context, XtnEntry xtnEntry, int templateOperation, MVCCEntryCacheInfo entryCacheInfo, MVCCEntryHolder updatedEntry, TypeData typeData) {
        MVCCShellEntryCacheInfo mvccShellEntryCacheInfo = cacheManager.getMVCCShellEntryCacheInfoByUid(updatedEntry.getUID());
        MVCCEntryHolder entryHolder = entryCacheInfo.getEntryHolder();
        updatedEntry.setWriteLockOwnerAndOperation(xtnEntry, templateOperation);
        MVCCEntryCacheInfo updatedEntryCacheInfo = new MVCCEntryCacheInfo(updatedEntry, entryCacheInfo.getBackRefs().size());
        updatedEntry.getTxnEntryData().setXidOriginated(xtnEntry);

        if(mvccShellEntryCacheInfo.getDirtyEntryCacheInfo() == entryCacheInfo){ //update dirty entry under same transaction
            IEntryData updatedEntryData = updatedEntry.getEntryData();
            cacheManager.updateEntryInCache(context, entryCacheInfo, entryHolder, updatedEntryData, updatedEntryData.getExpirationTime(), templateOperation);
        } else {
            entryHolder.setWriteLockOwnerAndOperation(xtnEntry, templateOperation);
            entryHolder.setMaybeUnderXtn(true);
            cacheManager.internalInsertEntryToCache(context, updatedEntry, true, typeData, updatedEntryCacheInfo, false);
        }
    }
}
