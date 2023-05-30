package com.j_spaces.core.cache.mvcc;

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
import com.j_spaces.core.server.transaction.EntryXtnInfo;

import java.util.concurrent.ConcurrentMap;

public class MVCCCacheManagerHandler {


    private final CacheManager cacheManager;

    public MVCCCacheManagerHandler(CacheManager cacheManager) {
        this.cacheManager = cacheManager;
    }

    public IEntryCacheInfo insertMvccEntryToCache(MVCCEntryCacheInfo pEntry, ConcurrentMap<String, IEntryCacheInfo> entries) throws SAException {
        String uid = pEntry.getUID();
        MVCCShellEntryCacheInfo oldEntry = (MVCCShellEntryCacheInfo) entries.get(uid);
        IEntryHolder entryHolder = pEntry.getEntryHolder();
        if (oldEntry == null){
            oldEntry = new MVCCShellEntryCacheInfo(entryHolder, pEntry);
            entries.put(uid,oldEntry);
        } else if (oldEntry.getDirtyEntryCacheInfo() == null) {
            MVCCEntryCacheInfo activeGeneration = oldEntry.getLatestGenerationCacheInfo();
            if (activeGeneration != null && activeGeneration.getEntryHolder().isLogicallyDeleted()) {
                XtnEntry xtnEntry = entryHolder.getXidOriginated();
                XtnData pXtn = xtnEntry.getXtnData();
                activeGeneration.getEntryHolder().setWriteLockOwnerAndOperation(entryHolder.getWriteLockOwner(), entryHolder.getWriteLockOperation());
                activeGeneration.getEntryHolder().getTxnEntryData().setXidOriginated(xtnEntry);
                pXtn.addMvccOverriddenActiveTakenEntries(activeGeneration);
            }
            oldEntry.setDirtyEntryCacheInfo(pEntry);
        } else{
            return oldEntry;
        }
        return null;
    }

    public void disconnectMvccEntryFromXtn(Context context, MVCCEntryCacheInfo pEntry, XtnEntry xtnEntry, boolean xtnEnd)
            throws SAException {
        if (pEntry == null)
            return; //no mvcc entry to disconnect
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

    public void handleDisconnectNewMvccEntryGenerationFromTransaction(Context context, MVCCEntryHolder entry, XtnEntry xtnEntry) throws SAException {
        MVCCEntryCacheInfo newMvccGenerationCacheInfo = xtnEntry.getXtnData().getMvccNewGenerationsEntries().get(entry.getUID());
        int writeLockOperation = newMvccGenerationCacheInfo != null
                ? newMvccGenerationCacheInfo.getEntryHolder().getWriteLockOperation() : SpaceOperations.NOOP;
        if (writeLockOperation != SpaceOperations.WRITE && writeLockOperation != SpaceOperations.UPDATE && writeLockOperation != SpaceOperations.TAKE){
            return;
        }
        MVCCEntryHolder newMvccGenerationEntryHolder = newMvccGenerationCacheInfo.getEntryHolder();
        if (writeLockOperation == SpaceOperations.TAKE &&
                newMvccGenerationEntryHolder.getCommittedGeneration() == entry.getOverrideGeneration() &&
                newMvccGenerationEntryHolder.isLogicallyDeleted()) {
                disconnectMvccEntryFromXtn(context, newMvccGenerationCacheInfo, xtnEntry, true);
        }
        if (writeLockOperation == SpaceOperations.UPDATE &&
                newMvccGenerationEntryHolder.getCommittedGeneration() == entry.getOverrideGeneration()){
            disconnectMvccEntryFromXtn(context, newMvccGenerationCacheInfo, xtnEntry, true);
        }
        if (writeLockOperation == SpaceOperations.WRITE &&
                newMvccGenerationEntryHolder.getOverrideGeneration() == entry.getCommittedGeneration()){
            disconnectMvccEntryFromXtn(context, newMvccGenerationCacheInfo, xtnEntry, true);
        }

    }

    public boolean isMvccEntryValidForWriteOnly(String uid) {
        return isMvccEntryValidForWriteOnly(cacheManager.getMVCCShellEntryCacheInfoByUid(uid));
    }

    public boolean isMvccEntryValidForWriteOnly(MVCCShellEntryCacheInfo shellEntryCacheInfo) {
        return shellEntryCacheInfo.getDirtyEntryCacheInfo() == null && shellEntryCacheInfo.isLogicallyDeletedOrEmpty();
    }

    public void createUpdateMvccEntryPendingGeneration(Context context, XtnEntry xtnEntry, int templateOperation, MVCCEntryCacheInfo entryCacheInfo, MVCCEntryHolder updatedEntry, TypeData typeData) throws SAException {
        MVCCShellEntryCacheInfo mvccShellEntryCacheInfo = cacheManager.getMVCCShellEntryCacheInfoByUid(updatedEntry.getUID());
        MVCCEntryHolder entryHolder = entryCacheInfo.getEntryHolder();
        updatedEntry.setWriteLockOwnerAndOperation(xtnEntry, templateOperation);
        updatedEntry.getTxnEntryData().setXidOriginated(xtnEntry);

        if(mvccShellEntryCacheInfo.getDirtyEntryCacheInfo() == entryCacheInfo){ //update dirty entry under same transaction as other operation
            if(entryCacheInfo.getEntryHolder().isLogicallyDeleted()) {
                //the dirty entry is logically deleted need to act like regular update on the active generation and ignore taken dirty entry
                disconnectMvccEntryFromXtn(context, entryCacheInfo, xtnEntry, false);
                entryCacheInfo = mvccShellEntryCacheInfo.getLatestGenerationCacheInfo();
                entryHolder = entryCacheInfo.getEntryHolder();
                mvccShellEntryCacheInfo.clearDirtyEntry();
                updatedEntry.setVersion(entryCacheInfo.getVersion() + 1); //the version should be incremented from the active version
            } else {
                //update dirty entry created by write or update operation, performing the update in-place on existing dirty entry
                IEntryData updatedEntryData = updatedEntry.getEntryData();
                cacheManager.updateEntryInCache(context, entryCacheInfo, entryHolder, updatedEntryData, updatedEntryData.getExpirationTime(), templateOperation);
                return;
            }
        }
        MVCCEntryCacheInfo updatedEntryCacheInfo = new MVCCEntryCacheInfo(updatedEntry, entryCacheInfo.getBackRefs().size());
        entryHolder.setWriteLockOwnerAndOperation(xtnEntry, templateOperation);
        entryHolder.setMaybeUnderXtn(true);
        cacheManager.internalInsertEntryToCache(context, updatedEntry, true, typeData, updatedEntryCacheInfo, false);

    }

    public MVCCEntryCacheInfo createLogicallyDeletedMvccEntryPendingGeneration(MVCCEntryHolder entryHolder) {
        MVCCShellEntryCacheInfo mvccShellEntryCacheInfo = cacheManager.getMVCCShellEntryCacheInfoByUid(entryHolder.getUID());
        EntryXtnInfo entryXtnInfo = entryHolder.getTxnEntryData().copyTxnInfo(true, false);
        MVCCEntryHolder dummyEntry = entryHolder.createLogicallyDeletedDummyEntry(entryXtnInfo);
        dummyEntry.setMaybeUnderXtn(true);
        MVCCEntryCacheInfo dirtyEntryCacheInfo = new MVCCEntryCacheInfo(dummyEntry, 2);
        mvccShellEntryCacheInfo.setDirtyEntryCacheInfo(dirtyEntryCacheInfo);
        return dirtyEntryCacheInfo;
    }

}
