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

    public IEntryCacheInfo insertMvccEntryToCache(Context context, MVCCEntryCacheInfo pEntry,
                                                  ConcurrentMap<String, IEntryCacheInfo> entries) throws SAException {
        String uid = pEntry.getUID();
        MVCCShellEntryCacheInfo existingShell = (MVCCShellEntryCacheInfo) entries.get(uid);
        MVCCShellEntryCacheInfo newShell = null;
        IEntryHolder newEntryToWrite = pEntry.getEntryHolder();
        if (existingShell == null) {
            newShell = new MVCCShellEntryCacheInfo(newEntryToWrite, pEntry);
            entries.put(uid, newShell);
        }
        if (context.isInMemoryRecovery()) {
            if (existingShell == null) { // write first - add dirty to gen queue
                newShell.addDirtyEntryToGenerationQueue();
            } else { // write second - add committed directly to gen queue
                existingShell.addCommittedEntryToGenerationQueue(pEntry);
            }
            return null;
        }

        if (existingShell != null && existingShell.getDirtyEntryCacheInfo() == null) {
            MVCCEntryCacheInfo latestGenerationCacheInfo = existingShell.getLatestGenerationCacheInfo();
            if (newEntryToWrite.getWriteLockOperation() == SpaceOperations.WRITE) {
                final boolean isLatestCommittedExist = latestGenerationCacheInfo != null;
                final boolean isLatestLogicallyDeleted = isLatestCommittedExist && latestGenerationCacheInfo.getEntryHolder().isLogicallyDeleted();
                if (isLatestLogicallyDeleted) { // this is new write
                    XtnEntry xtnEntry = newEntryToWrite.getXidOriginated();
                    XtnData pXtn = xtnEntry.getXtnData();
                    latestGenerationCacheInfo.getEntryHolder().setWriteLockOwnerAndOperation(newEntryToWrite.getWriteLockOwner(), newEntryToWrite.getWriteLockOperation());
                    pXtn.addWriteActiveLogicallyDeletedEntry(latestGenerationCacheInfo);
                } else if (isLatestCommittedExist) { //it already exists and not logically deleted.
                    return existingShell;
                }
            } // else its write operation on empty shell / update operation on active data
            existingShell.setDirtyEntryCacheInfo(pEntry);
            return null;
        }
        return existingShell;
}

    public void disconnectMVCCEntryFromXtn(Context context, MVCCEntryCacheInfo pEntry, XtnEntry xtnEntry, boolean xtnEnd)
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

    public void handleDisconnectOldLogicallyDeletedMVCCEntryGenerationFromTransaction(Context context, MVCCEntryHolder entry, XtnEntry xtnEntry) throws SAException {
        if (entry.getWriteLockOperation() != SpaceOperations.WRITE) { // disconnecting only for write operation for uid
            return;
        }
        MVCCEntryCacheInfo oldLogicallyDeletedMVCCGenerationCacheInfo = xtnEntry.getXtnData().getMvccWriteActiveLogicallyDeletedEntry(entry.getUID());
        if (oldLogicallyDeletedMVCCGenerationCacheInfo == null) { // check that logically deleted entry exists with same uid
            return;
        }
        MVCCEntryHolder logicallyDeletedEntryHolder = oldLogicallyDeletedMVCCGenerationCacheInfo.getEntryHolder();
        if (logicallyDeletedEntryHolder.getOverrideGeneration() == entry.getCommittedGeneration()) { // check that deleted entry was overridden by written entry
            disconnectMVCCEntryFromXtn(context, oldLogicallyDeletedMVCCGenerationCacheInfo, xtnEntry, true);
        }
    }


    public void handleDisconnectNewMVCCEntryGenerationFromTransaction(Context context, MVCCEntryHolder entry, XtnEntry xtnEntry) throws SAException {
        MVCCEntryCacheInfo newMVCCGenerationCacheInfo = xtnEntry.getXtnData().getMvccNewGenerationsEntry(entry.getUID());
        int writeLockOperation = newMVCCGenerationCacheInfo != null
                ? newMVCCGenerationCacheInfo.getEntryHolder().getWriteLockOperation() : SpaceOperations.NOOP;

        if (writeLockOperation != SpaceOperations.WRITE && writeLockOperation != SpaceOperations.UPDATE
                && writeLockOperation != SpaceOperations.TAKE && writeLockOperation != SpaceOperations.TAKE_IE) {
            return;
        }
        MVCCEntryHolder newMvccGenerationEntryHolder = newMVCCGenerationCacheInfo.getEntryHolder();
        // TAKE_IE operation only for backup
        if ((writeLockOperation == SpaceOperations.TAKE || writeLockOperation == SpaceOperations.TAKE_IE) &&
                newMvccGenerationEntryHolder.getCommittedGeneration() == entry.getOverrideGeneration() &&
                newMvccGenerationEntryHolder.isLogicallyDeleted()) {
            disconnectMVCCEntryFromXtn(context, newMVCCGenerationCacheInfo, xtnEntry, true);
        }
        if (writeLockOperation == SpaceOperations.UPDATE &&
                newMvccGenerationEntryHolder.getCommittedGeneration() == entry.getOverrideGeneration()) {
            disconnectMVCCEntryFromXtn(context, newMVCCGenerationCacheInfo, xtnEntry, true);
        }
        if (writeLockOperation == SpaceOperations.WRITE &&
                newMvccGenerationEntryHolder.getOverrideGeneration() == entry.getCommittedGeneration()) {
            disconnectMVCCEntryFromXtn(context, newMVCCGenerationCacheInfo, xtnEntry, true);
        }

    }

    public void createUpdateMvccEntryPendingGeneration(Context context, XtnEntry xtnEntry, int templateOperation, MVCCEntryCacheInfo entryCacheInfo, MVCCEntryHolder updatedEntry, TypeData typeData) throws SAException {
        MVCCShellEntryCacheInfo mvccShellEntryCacheInfo = cacheManager.getMVCCShellEntryCacheInfoByUid(updatedEntry.getUID());
        MVCCEntryHolder entryHolder = entryCacheInfo.getEntryHolder();

        if (mvccShellEntryCacheInfo.getDirtyEntryCacheInfo() == entryCacheInfo) {
            //update dirty entry created by write or update operation, performing the update in-place on existing dirty entry
            IEntryData updatedEntryData = updatedEntry.getEntryData();
            cacheManager.updateEntryInCache(context, entryCacheInfo, entryHolder, updatedEntryData, updatedEntryData.getExpirationTime(), templateOperation);
            return;
        }
        MVCCEntryCacheInfo updatedEntryCacheInfo = new MVCCEntryCacheInfo(updatedEntry, entryCacheInfo.getBackRefs().size());
        entryHolder.setWriteLockOwnerAndOperation(xtnEntry, templateOperation);
        entryHolder.setMaybeUnderXtn(true);
        cacheManager.internalInsertEntryToCache(context, updatedEntry, true, typeData, updatedEntryCacheInfo, false);
    }

    public MVCCEntryCacheInfo createLogicallyDeletedMvccEntryPendingGeneration(XtnEntry xtnEntry, MVCCEntryCacheInfo pEntry, int operationId) {
        MVCCShellEntryCacheInfo mvccShellEntryCacheInfo = cacheManager.getMVCCShellEntryCacheInfoByUid(pEntry.getUID());
        MVCCEntryHolder entryHolder = pEntry.getEntryHolder();

        entryHolder.setWriteLockOperation(operationId, false);

        EntryXtnInfo entryXtnInfo = entryHolder.getTxnEntryData().copyTxnInfo(true, false);
        entryXtnInfo.setXidOriginated(xtnEntry);

        MVCCEntryHolder dummyEntry = entryHolder.createLogicallyDeletedDummyEntry(entryXtnInfo);
        dummyEntry.setMaybeUnderXtn(true);
        MVCCEntryCacheInfo dirtyEntryCacheInfo = new MVCCEntryCacheInfo(dummyEntry, 2);
        mvccShellEntryCacheInfo.setDirtyEntryCacheInfo(dirtyEntryCacheInfo);
        return dirtyEntryCacheInfo;
    }
}
