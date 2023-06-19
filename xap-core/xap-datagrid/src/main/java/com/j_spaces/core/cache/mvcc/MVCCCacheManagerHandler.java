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
        MVCCShellEntryCacheInfo existingShell = (MVCCShellEntryCacheInfo) entries.get(uid);
        IEntryHolder newEntryToWrite = pEntry.getEntryHolder();
        if (existingShell == null) {
            entries.put(uid, new MVCCShellEntryCacheInfo(newEntryToWrite, pEntry));
        } else if (existingShell.getDirtyEntryCacheInfo() == null) {
            MVCCEntryCacheInfo latestGenerationCacheInfo = existingShell.getLatestGenerationCacheInfo();
            if (newEntryToWrite.getWriteLockOperation() == SpaceOperations.WRITE) {
                final boolean isLatestCommittedExist = latestGenerationCacheInfo != null;
                final boolean isLatestLogicallyDeleted = isLatestCommittedExist && latestGenerationCacheInfo.getEntryHolder().isLogicallyDeleted();
                if (isLatestLogicallyDeleted) { // this is new write
                    XtnEntry xtnEntry = newEntryToWrite.getXidOriginated();
                    XtnData pXtn = xtnEntry.getXtnData();
                    latestGenerationCacheInfo.getEntryHolder().setWriteLockOwnerAndOperation(newEntryToWrite.getWriteLockOwner(), newEntryToWrite.getWriteLockOperation());
                    pXtn.addWriteActiveLogicallyDeletedEntries(latestGenerationCacheInfo);
                } else if (isLatestCommittedExist) { //it already exists and not logically deleted.
                    return existingShell;
                }
            } // else its write operation on empty shell / update operation on active data
            existingShell.setDirtyEntryCacheInfo(pEntry);
            return null;
        }
        return existingShell;
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
        if (writeLockOperation != SpaceOperations.WRITE && writeLockOperation != SpaceOperations.UPDATE && writeLockOperation != SpaceOperations.TAKE) {
            return;
        }
        MVCCEntryHolder newMvccGenerationEntryHolder = newMvccGenerationCacheInfo.getEntryHolder();
        if (writeLockOperation == SpaceOperations.TAKE &&
                newMvccGenerationEntryHolder.getCommittedGeneration() == entry.getOverrideGeneration() &&
                newMvccGenerationEntryHolder.isLogicallyDeleted()) {
            disconnectMvccEntryFromXtn(context, newMvccGenerationCacheInfo, xtnEntry, true);
        }
        if (writeLockOperation == SpaceOperations.UPDATE &&
                newMvccGenerationEntryHolder.getCommittedGeneration() == entry.getOverrideGeneration()) {
            disconnectMvccEntryFromXtn(context, newMvccGenerationCacheInfo, xtnEntry, true);
        }
        if (writeLockOperation == SpaceOperations.WRITE &&
                newMvccGenerationEntryHolder.getOverrideGeneration() == entry.getCommittedGeneration()) {
            disconnectMvccEntryFromXtn(context, newMvccGenerationCacheInfo, xtnEntry, true);
        }

    }

    public void createUpdateMvccEntryPendingGeneration(Context context, XtnEntry xtnEntry, int templateOperation, MVCCEntryCacheInfo entryCacheInfo, MVCCEntryHolder updatedEntry, TypeData typeData) throws SAException {
        MVCCShellEntryCacheInfo mvccShellEntryCacheInfo = cacheManager.getMVCCShellEntryCacheInfoByUid(updatedEntry.getUID());
        MVCCEntryHolder entryHolder = entryCacheInfo.getEntryHolder();
        updatedEntry.setWriteLockOwnerAndOperation(xtnEntry, templateOperation);
        updatedEntry.getTxnEntryData().setXidOriginated(xtnEntry);

        if (mvccShellEntryCacheInfo.getDirtyEntryCacheInfo() == entryCacheInfo) { //update dirty entry under same transaction as other operation
            if (entryCacheInfo.getEntryHolder().isLogicallyDeleted()) {
                //the dirty entry is logically deleted need to act like regular update on the active generation and ignore taken dirty entry
                disconnectMvccEntryFromXtn(context, entryCacheInfo, xtnEntry, false);
                entryCacheInfo = mvccShellEntryCacheInfo.getLatestGenerationCacheInfo();
                entryHolder = entryCacheInfo.getEntryHolder();
                xtnEntry.getXtnData().removeTakenEntry(entryCacheInfo);
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

    public MVCCEntryCacheInfo createLogicallyDeletedMvccEntryPendingGeneration(Context context, XtnEntry xtnEntry, MVCCEntryCacheInfo pEntry, int operationId) throws SAException {
        MVCCShellEntryCacheInfo mvccShellEntryCacheInfo = cacheManager.getMVCCShellEntryCacheInfoByUid(pEntry.getUID());
        MVCCEntryHolder entryHolder = pEntry.getEntryHolder();

        if (mvccShellEntryCacheInfo.getDirtyEntryCacheInfo() != null) {
            disconnectMvccEntryFromXtn(context, pEntry, xtnEntry, false);// disconnecting old data from txn

            xtnEntry.getXtnData().removeUpdatedEntry(pEntry);
            xtnEntry.getXtnData().removeTakenEntry(pEntry);

            pEntry = mvccShellEntryCacheInfo.getLatestGenerationCacheInfo();
            entryHolder = pEntry.getEntryHolder();

            xtnEntry.getXtnData().addToTakenEntriesIfNotInside(pEntry);

            mvccShellEntryCacheInfo.clearDirtyEntry();
            entryHolder.setWriteLockOperation(operationId, false /*createSnapshot*/);
        }

        EntryXtnInfo entryXtnInfo = entryHolder.getTxnEntryData().copyTxnInfo(true, false);
        entryXtnInfo.setXidOriginated(xtnEntry);

        MVCCEntryHolder dummyEntry = entryHolder.createLogicallyDeletedDummyEntry(entryXtnInfo);// has to be from pEntry
        dummyEntry.setMaybeUnderXtn(true);
        MVCCEntryCacheInfo dirtyEntryCacheInfo = new MVCCEntryCacheInfo(dummyEntry, 2);
        mvccShellEntryCacheInfo.setDirtyEntryCacheInfo(dirtyEntryCacheInfo);
        return dirtyEntryCacheInfo;
    }
}
