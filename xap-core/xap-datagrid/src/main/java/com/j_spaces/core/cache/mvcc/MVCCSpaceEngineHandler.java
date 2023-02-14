package com.j_spaces.core.cache.mvcc;

import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.space.mvcc.MVCCGenerationsState;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.j_spaces.core.*;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.sadapter.ISAdapterIterator;
import com.j_spaces.core.sadapter.SAException;
import com.j_spaces.core.sadapter.SelectType;

public class MVCCSpaceEngineHandler {

    private final SpaceEngine _spaceEngine;
    private final CacheManager _cacheManager;

    public MVCCSpaceEngineHandler(SpaceEngine spaceEngine) {
        _spaceEngine = spaceEngine;
        _cacheManager = spaceEngine.getCacheManager();
    }

    public void commitMVCCEntries(Context context, XtnEntry xtnEntry) throws SAException {
        final MVCCGenerationsState mvccGenerationsState = xtnEntry.getMVCCGenerationsState();
        final long nextGeneration = mvccGenerationsState.getNextGeneration();
        ISAdapterIterator<IEntryHolder> entriesIter = null;
        entriesIter = _cacheManager.makeUnderXtnEntriesIter(context,
                xtnEntry, SelectType.ALL_ENTRIES, false /* returnPEntry*/);
        if (entriesIter != null) {
            while (true) {
                MVCCEntryHolder entry = (MVCCEntryHolder) entriesIter.next();
                if (entry == null)
                    break;
                MVCCShellEntryCacheInfo mvccShellEntryCacheInfo = (MVCCShellEntryCacheInfo) _cacheManager.getPEntryByUid(entry.getUID());
                if (entry.getWriteLockOwner() == xtnEntry) {
                    entry.setCommittedGeneration(nextGeneration);
                    mvccShellEntryCacheInfo.addEntryGeneration();
                }
                if (entry.anyReadLockXtn() && entry.getReadLockOwners().contains(xtnEntry)) {
                    // todo: right now do nothing.
                }
            }
        }
    }

    public IEntryHolder getMatchedEntryAndOperateSA_Entry(Context context,
                                                          ITemplateHolder template,
                                                          boolean makeWaitForInfo,
                                                          MVCCEntryHolder entry) throws TemplateDeletedException, TransactionNotActiveException, TransactionConflictException, FifoException, SAException, NoMatchException, EntryDeletedException {
        final XtnEntry xidOriginated = template.getXidOriginated();
        MVCCGenerationsState mvccGenerationsState = null;
        if (xidOriginated != null) {
           mvccGenerationsState = xidOriginated.getMVCCGenerationsState();
        } else {
            // TODO: no transaction - get from context.
            // mvccGenerationsState = context.getMVCCGenerationsState()
        }

        final long completedGeneration = mvccGenerationsState.getCompletedGeneration();
        final long overrideGeneration = entry.getOverrideGeneration();
        final long committedGeneration = entry.getCommittedGeneration();
        final boolean logicallyDeleted = entry.isLogicallyDeleted();
        if (
                (!logicallyDeleted)
                && (committedGeneration != -1)
                && (committedGeneration <= completedGeneration)
                && (!mvccGenerationsState.isUncompletedGeneration(committedGeneration))
                && ((overrideGeneration == -1)
                    || (overrideGeneration > completedGeneration)
                    || (overrideGeneration <= committedGeneration && mvccGenerationsState.isUncompletedGeneration(overrideGeneration)))
        ) {
            _spaceEngine.performTemplateOnEntrySA(context, template, entry,
                    makeWaitForInfo);
            return entry;
        }
        return null; // continue
    }
}
