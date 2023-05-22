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
import com.j_spaces.core.server.transaction.EntryXtnInfo;
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
                        if (entry.getWriteLockOperation() == SpaceOperations.TAKE && entry.getWriteLockOwner() == xtnEntry) {
                            entry.setOverrideGeneration(nextGeneration);
                            entry.resetEntryXtnInfo();
                            entry.setMaybeUnderXtn(true);
                            MVCCEntryHolder dirtyEntryHolder = mvccShellEntryCacheInfo.getDirtyEntryHolder();
                            dirtyEntryHolder.setCommittedGeneration(nextGeneration);
                            dirtyEntryHolder.setOverridingAnother(true);
                            mvccShellEntryCacheInfo.addDirtyEntryToGenerationQueue();
                        } else if (entry.getWriteLockOperation() == SpaceOperations.WRITE && entry.getWriteLockOwner() == xtnEntry) {
                            entry.setCommittedGeneration(nextGeneration);
                            mvccShellEntryCacheInfo.addDirtyEntryToGenerationQueue();
                        }
                    }
                } finally {
                    _cacheManager.getLockManager().freeLockObject(entryLock);
                }
            }
        }
    }

    public IEntryHolder getMatchedMVCCEntry(ITemplateHolder template,
                                                        IEntryCacheInfo entryCacheInfo) {
        if (entryCacheInfo instanceof MVCCShellEntryCacheInfo) {
            MVCCShellEntryCacheInfo shellEntry = (MVCCShellEntryCacheInfo)entryCacheInfo;
            final Iterator<MVCCEntryCacheInfo> generationIterator = shellEntry.descIterator();
            if (!generationIterator.hasNext() && shellEntry.getDirtyEntryCacheInfo() != null) {
                return getEntryIfMatched(template, shellEntry.getDirtyEntryHolder());
            }
            while (generationIterator.hasNext()) {
                final MVCCEntryHolder entryHolder = generationIterator.next().getEntryHolder();
                final MVCCEntryHolder matchMvccEntryHolder = getEntryIfMatched(template, entryHolder);
                if (matchMvccEntryHolder != null) return matchMvccEntryHolder;
            }
            return null; // continue
        }
        return getEntryIfMatched(template, (MVCCEntryHolder)entryCacheInfo.getEntryHolder(_cacheManager));

    }

    private MVCCEntryHolder getEntryIfMatched(ITemplateHolder template, MVCCEntryHolder entryHolder) {
        if (entryHolder.isLogicallyDeleted() || !match(template, entryHolder)) {
            return null;
        }
        return entryHolder;
    }

    private boolean match(ITemplateHolder template, MVCCEntryHolder entryHolder) {
        return (template.isActiveRead(_spaceEngine) && entryHolder.getOverrideGeneration() == -1)
                || (template.getGenerationsState() != null && isEntryMatchedByGenerationsState(template, entryHolder))
                || entryHolder.getCommittedGeneration() == -1; /*dirty entry*/
    }

    private boolean isEntryMatchedByGenerationsState(ITemplateHolder template, MVCCEntryHolder entryHolder) {
        final MVCCGenerationsState mvccGenerationsState = template.getGenerationsState();
        final long completedGeneration = mvccGenerationsState.getCompletedGeneration();
        final long overrideGeneration = entryHolder.getOverrideGeneration();
        final long committedGeneration = entryHolder.getCommittedGeneration();
        if (template.isReadOperation()) {
            return ((committedGeneration != -1)
                    && (committedGeneration <= completedGeneration)
                    && (!mvccGenerationsState.isUncompletedGeneration(committedGeneration))
                    && ((overrideGeneration == -1)
                    || (overrideGeneration > completedGeneration)
                    || (overrideGeneration <= completedGeneration && mvccGenerationsState.isUncompletedGeneration(overrideGeneration))));
        } else {
            return (committedGeneration != -1)
                    && (committedGeneration <= completedGeneration)
                    && (!mvccGenerationsState.isUncompletedGeneration(committedGeneration))
                    && (overrideGeneration == -1);
        }
    }

    public SpaceEngine.XtnConflictCheckIndicators checkTransactionConflict(Context context, MVCCEntryHolder entry, ITemplateHolder template) {
        if ((template.getTemplateOperation() == SpaceOperations.TAKE_IE || template.getTemplateOperation() == SpaceOperations.TAKE)) {
            if (entry.isLogicallyDeleted()) {
                if (_spaceEngine.getLogger().isDebugEnabled()) {
                    _spaceEngine.getLogger().debug("Encountered a conflict while attempting to take " + entry
                            + ", this entry is logically deleted."
                            + " the current generation state is " + template.getGenerationsState());
                }
                return SpaceEngine.XtnConflictCheckIndicators.ENTRY_DELETED;
            } else if (entry.getOverrideGeneration() > -1) {
                if (_spaceEngine.getLogger().isDebugEnabled()) {
                    _spaceEngine.getLogger().debug("Encountered a conflict while attempting to take " + entry
                            + ", this entry has already overridden by another generation."
                            + " the current generation state is " + template.getGenerationsState());
                }
                return SpaceEngine.XtnConflictCheckIndicators.XTN_CONFLICT;
            }
        }
        return SpaceEngine.XtnConflictCheckIndicators.NO_CONFLICT;
    }


}
