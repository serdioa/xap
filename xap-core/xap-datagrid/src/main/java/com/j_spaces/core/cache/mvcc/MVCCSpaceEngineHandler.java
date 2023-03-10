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

import java.util.Iterator;

public class MVCCSpaceEngineHandler {

    private final SpaceEngine _spaceEngine;
    private final CacheManager _cacheManager;

    public MVCCSpaceEngineHandler(SpaceEngine spaceEngine) {
        _spaceEngine = spaceEngine;
        _cacheManager = spaceEngine.getCacheManager();
    }

    public void commitMVCCEntries(Context context, XtnEntry xtnEntry) throws SAException {
        final MVCCGenerationsState mvccGenerationsState = xtnEntry.getMVCCGenerationsState();
        if (mvccGenerationsState == null) return;
        final long nextGeneration = mvccGenerationsState.getNextGeneration();
        ISAdapterIterator<IEntryHolder> entriesIter = null;
        entriesIter = _cacheManager.makeUnderXtnEntriesIter(context,
                xtnEntry, SelectType.ALL_ENTRIES, false /* returnPEntry*/);
        if (entriesIter != null) {
            while (true) {
                MVCCEntryHolder entry = (MVCCEntryHolder) entriesIter.next();
                if (entry == null)
                    break;
                MVCCShellEntryCacheInfo mvccShellEntryCacheInfo = _cacheManager.getMVCCShellEntryCacheInfoByUid(entry.getUID());
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
                                                          MVCCShellEntryCacheInfo shellEntry) throws TemplateDeletedException, TransactionNotActiveException, TransactionConflictException, FifoException, SAException, NoMatchException, EntryDeletedException {
        final MVCCGenerationsState mvccGenerationsState = getMvccGenerationsState(context, template);
        final Iterator<MVCCEntryCacheInfo> generationIterator = shellEntry.descIterator();
        if (!generationIterator.hasNext() && shellEntry.getDirtyEntry() != null) {
            return getMatchMvccEntryHolder(context, template, makeWaitForInfo,
                    (MVCCEntryHolder) shellEntry.getDirtyEntry().getEntryHolder(), mvccGenerationsState);
        }
        while (generationIterator.hasNext()) {
            final MVCCEntryCacheInfo entryCacheInfo = generationIterator.next();
            final MVCCEntryHolder entryHolder = (MVCCEntryHolder) entryCacheInfo.getEntryHolder();
            final MVCCEntryHolder matchMvccEntryHolder = getMatchMvccEntryHolder(context, template, makeWaitForInfo,
                    entryHolder, mvccGenerationsState);
            if (matchMvccEntryHolder != null) return matchMvccEntryHolder;
        }
        return null; // continue
    }


    public IEntryHolder getMatchedEntryAndOperateSA_Entry(Context context,
                                                          ITemplateHolder template,
                                                          boolean makeWaitForInfo,
                                                          MVCCEntryHolder entryHolder) throws TemplateDeletedException,
            TransactionNotActiveException, TransactionConflictException, FifoException, SAException, NoMatchException, EntryDeletedException {
        final MVCCGenerationsState mvccGenerationsState = getMvccGenerationsState(context, template);
        return getMatchMvccEntryHolder(context, template, makeWaitForInfo, entryHolder, mvccGenerationsState);
    }

    private static MVCCGenerationsState getMvccGenerationsState(Context context, ITemplateHolder template) {
        final XtnEntry xidOriginated = template.getXidOriginated();
        MVCCGenerationsState mvccGenerationsState = null;
        if (xidOriginated != null) {
            mvccGenerationsState = xidOriginated.getMVCCGenerationsState();
        } else {
            // TODO: no transaction - get from context.
            // mvccGenerationsState = context.getMVCCGenerationsState()
        }
        return mvccGenerationsState;
    }

    private MVCCEntryHolder getMatchMvccEntryHolder(Context context, ITemplateHolder template, boolean makeWaitForInfo, MVCCEntryHolder entryHolder, MVCCGenerationsState mvccGenerationsState) throws TransactionConflictException, EntryDeletedException, TemplateDeletedException, TransactionNotActiveException, SAException, NoMatchException, FifoException {
        if (template.isActiveRead(_spaceEngine)) {
            _spaceEngine.performTemplateOnEntrySA(context, template, entryHolder, makeWaitForInfo);
            return entryHolder;
        }
        final long completedGeneration = mvccGenerationsState.getCompletedGeneration();
        final long overrideGeneration = entryHolder.getOverrideGeneration();
        final long committedGeneration = entryHolder.getCommittedGeneration();
        final boolean logicallyDeleted = entryHolder.isLogicallyDeleted();
        if (isEntryMatchedByGenerationsState(mvccGenerationsState, completedGeneration, overrideGeneration, committedGeneration)) {
            _spaceEngine.performTemplateOnEntrySA(context, template, entryHolder, makeWaitForInfo);
            return entryHolder;
        }
        return null; // continue
    }

    private boolean isEntryMatchedByGenerationsState(MVCCGenerationsState mvccGenerationsState, long completedGeneration, long overrideGeneration, long committedGeneration) {
        return ((committedGeneration != -1)
                && (committedGeneration <= completedGeneration)
                && (!mvccGenerationsState.isUncompletedGeneration(committedGeneration))
                && ((overrideGeneration == -1)
                    || (overrideGeneration > completedGeneration)
                    || (overrideGeneration <= committedGeneration && mvccGenerationsState.isUncompletedGeneration(overrideGeneration))));
    }
}
