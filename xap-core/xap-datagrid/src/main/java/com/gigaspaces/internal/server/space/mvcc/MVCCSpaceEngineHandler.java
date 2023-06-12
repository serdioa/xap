package com.gigaspaces.internal.server.space.mvcc;

import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.j_spaces.core.SpaceOperations;
import com.j_spaces.core.XtnEntry;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.cache.mvcc.MVCCEntryCacheInfo;
import com.j_spaces.core.cache.mvcc.MVCCEntryHolder;
import com.j_spaces.core.cache.mvcc.MVCCShellEntryCacheInfo;
import com.j_spaces.core.sadapter.ISAdapterIterator;
import com.j_spaces.core.sadapter.SAException;
import com.j_spaces.core.sadapter.SelectType;
import com.j_spaces.kernel.locks.ILockObject;

public class MVCCSpaceEngineHandler {

    private final SpaceEngine _spaceEngine;
    private final CacheManager _cacheManager;

    public MVCCSpaceEngineHandler(SpaceEngine spaceEngine) {
        _spaceEngine = spaceEngine;
        _cacheManager = spaceEngine.getCacheManager();
    }

    public void preCommitMvccEntries(Context context, XtnEntry xtnEntry) throws SAException {
        final MVCCGenerationsState mvccGenerationsState = xtnEntry.getMVCCGenerationsState();
        if (mvccGenerationsState == null) return;
        final long nextGeneration = mvccGenerationsState.getNextGeneration();
        ISAdapterIterator<IEntryHolder> entriesIter = null;
        entriesIter = _cacheManager.makeUnderXtnEntriesIter(context,
                xtnEntry, SelectType.ALL_ENTRIES, false /* returnPEntry*/);
        ILockObject entryLock = null;
        if (entriesIter != null) {
            while (true) {
                MVCCEntryHolder entry = (MVCCEntryHolder) entriesIter.next();
                if (entry == null) {
                    break;
                }
                entryLock = _cacheManager.getLockManager().getLockObject(entry);
                try {
                    synchronized (entryLock) {
                        MVCCShellEntryCacheInfo mvccShellEntryCacheInfo = _cacheManager.getMVCCShellEntryCacheInfoByUid(entry.getUID());
                        MVCCEntryHolder dirtyEntryHolder = mvccShellEntryCacheInfo.getDirtyEntryHolder();
                        int writeLockOperation = entry.getWriteLockOperation();
                        if(entry.getWriteLockOwner() == xtnEntry) {
                            switch (writeLockOperation) {
                                case SpaceOperations.TAKE:
                                case SpaceOperations.UPDATE:
                                    entry.setOverrideGeneration(nextGeneration);
                                    entry.resetEntryXtnInfo();
                                    entry.setMaybeUnderXtn(true);
                                    dirtyEntryHolder.setOverridingAnother(true);
                                    dirtyEntryHolder.setCommittedGeneration(nextGeneration);
                                    mvccShellEntryCacheInfo.addDirtyEntryToGenerationQueue();
                                    break;
                                case SpaceOperations.WRITE:
                                    MVCCEntryCacheInfo activeTakenGenerationEntry = xtnEntry.getXtnData().getMvccOverriddenActiveTakenEntry(entry.getUID());
                                    if (activeTakenGenerationEntry != null){ //performing a write operation on a taken entry generation
                                        MVCCEntryHolder activeEntryHolder = activeTakenGenerationEntry.getEntryHolder();
                                        activeEntryHolder.setOverrideGeneration(nextGeneration);
                                        activeEntryHolder.resetEntryXtnInfo();
                                        activeEntryHolder.setMaybeUnderXtn(true);
                                        entry.setOverridingAnother(true);
                                    }
                                    entry.setCommittedGeneration(nextGeneration);
                                    mvccShellEntryCacheInfo.addDirtyEntryToGenerationQueue();
                                    break;
                            }
                        }
                    }
                } finally {
                    _cacheManager.getLockManager().freeLockObject(entryLock);
                }
            }
        }
    }

    public SpaceEngine.XtnConflictCheckIndicators checkTransactionConflict(MVCCEntryHolder entry, ITemplateHolder template) {
        int templateOperation = template.getTemplateOperation();
        switch (templateOperation) {
            case SpaceOperations.TAKE_IE:
            case SpaceOperations.TAKE:
            case SpaceOperations.UPDATE:
                if (entry.getOverrideGeneration() > -1) {
                    if (_spaceEngine.getLogger().isDebugEnabled()) {
                        _spaceEngine.getLogger().debug("Encountered a conflict while attempting to modify " + entry
                                + ", this entry has already overridden by another generation."
                                + " the current generation state is " + template.getGenerationsState());
                    }
                    return SpaceEngine.XtnConflictCheckIndicators.XTN_CONFLICT;
                }
        }
        return SpaceEngine.XtnConflictCheckIndicators.NO_CONFLICT;
    }


}
