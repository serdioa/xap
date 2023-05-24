package com.gigaspaces.internal.server.space.mvcc;

import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.j_spaces.core.SpaceOperations;
import com.j_spaces.core.XtnEntry;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.IEntryCacheInfo;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.cache.mvcc.MVCCEntryCacheInfo;
import com.j_spaces.core.cache.mvcc.MVCCEntryHolder;
import com.j_spaces.core.cache.mvcc.MVCCShellEntryCacheInfo;
import com.j_spaces.core.sadapter.ISAdapterIterator;
import com.j_spaces.core.sadapter.SAException;
import com.j_spaces.core.sadapter.SelectType;
import com.j_spaces.kernel.locks.ILockObject;

import java.util.Iterator;

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

    public IEntryHolder getMVCCEntryIfMatched(ITemplateHolder template,
                                                        IEntryCacheInfo entryCacheInfo) {
        if (entryCacheInfo instanceof MVCCShellEntryCacheInfo) {
            MVCCShellEntryCacheInfo shellEntry = (MVCCShellEntryCacheInfo)entryCacheInfo;
            MVCCEntryHolder dirtyEntryHolder = shellEntry.getDirtyEntryHolder();
            if (dirtyEntryHolder != null) {
                MVCCEntryHolder entryHolder = getMVCCEntryIfMatched(template, dirtyEntryHolder);
                if (entryHolder != null){
                    return entryHolder;
                }
            }
            final Iterator<MVCCEntryCacheInfo> generationIterator = shellEntry.descIterator();
            while (generationIterator.hasNext()) {
                final MVCCEntryHolder entryHolder = generationIterator.next().getEntryHolder();
                final MVCCEntryHolder matchMvccEntryHolder = getMVCCEntryIfMatched(template, entryHolder);
                if (matchMvccEntryHolder != null) return matchMvccEntryHolder;
            }
            return null; // continue
        }
        return getMVCCEntryIfMatched(template, (MVCCEntryHolder)entryCacheInfo.getEntryHolder(_cacheManager));

    }

    private MVCCEntryHolder getMVCCEntryIfMatched(ITemplateHolder template, MVCCEntryHolder entryHolder) {
        if (entryHolder.isLogicallyDeleted() || !isEntryMatchedByGenerationsState(entryHolder, template)) {
            return null;
        }
        return entryHolder;
    }

    private boolean isEntryMatchedByGenerationsState(MVCCEntryHolder entryHolder, ITemplateHolder template) {
        final MVCCGenerationsState mvccGenerationsState = template.getGenerationsState();
        final long completedGeneration = mvccGenerationsState == null ? -1 : mvccGenerationsState.getCompletedGeneration();
        final long overrideGeneration = entryHolder.getOverrideGeneration();
        final long committedGeneration = entryHolder.getCommittedGeneration();
        if (template.isReadOperation()) {
            if (template.isActiveRead(_spaceEngine)){
                return committedGeneration == -1 || overrideGeneration == -1;
            } else{
                return ((committedGeneration != -1)
                        && (committedGeneration <= completedGeneration)
                        && (!mvccGenerationsState.isUncompletedGeneration(committedGeneration))
                        && ((overrideGeneration == -1)
                        || (overrideGeneration > completedGeneration)
                        || (overrideGeneration <= completedGeneration && mvccGenerationsState.isUncompletedGeneration(overrideGeneration))));
            }
        } else {
            boolean isDirtyEntry = committedGeneration == -1 && overrideGeneration == -1;
            return isDirtyEntry
                    || ((committedGeneration != -1)
                    && (committedGeneration <= completedGeneration)
                    && (!mvccGenerationsState.isUncompletedGeneration(committedGeneration))
                    && (overrideGeneration == -1));
        }
    }

    public SpaceEngine.XtnConflictCheckIndicators checkTransactionConflict(Context context, MVCCEntryHolder entry, ITemplateHolder template) {
        if (template.getTemplateOperation() == SpaceOperations.TAKE_IE
                || template.getTemplateOperation() == SpaceOperations.TAKE
                || template.getTemplateOperation() == SpaceOperations.UPDATE) {
            if (entry.isLogicallyDeleted()) {
                if (_spaceEngine.getLogger().isDebugEnabled()) {
                    _spaceEngine.getLogger().debug("Encountered a conflict while attempting to modify " + entry
                            + ", this entry is logically deleted."
                            + " the current generation state is " + template.getGenerationsState());
                }
                return SpaceEngine.XtnConflictCheckIndicators.ENTRY_DELETED;
            } else if (entry.getOverrideGeneration() > -1) {
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
