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
import com.j_spaces.kernel.locks.ILockObject;

import java.util.Iterator;

public class MVCCSpaceEngineHandler {

    private final SpaceEngine _spaceEngine;
    private final CacheManager _cacheManager;

    public MVCCSpaceEngineHandler(SpaceEngine spaceEngine) {
        _spaceEngine = spaceEngine;
        _cacheManager = spaceEngine.getCacheManager();
    }

    public void prepareMVCCEntries(Context context, XtnEntry xtnEntry) throws SAException {
        final MVCCGenerationsState mvccGenerationsState = xtnEntry.getMVCCGenerationsState();
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
                        if (entry.getWriteLockOperation() == SpaceOperations.WRITE && entry.getWriteLockOwner() == xtnEntry) {
                            entry.setCommittedGeneration(nextGeneration);
                        }
                        if (entry.anyReadLockXtn() && entry.getReadLockOwners().contains(xtnEntry)) {
                            // todo: right now do nothing.
                        }
                    }
                } finally {
                    _cacheManager.getLockManager().freeLockObject(entryLock);
                }
            }
        }
    }

    public void commitMVCCEntries(Context context, XtnEntry xtnEntry) throws SAException {
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
                synchronized (entryLock) {
                    try {
                        MVCCShellEntryCacheInfo mvccShellEntryCacheInfo = (MVCCShellEntryCacheInfo) _cacheManager.getPEntryByUid(entry.getUID());
                        if (entry.getWriteLockOperation() == SpaceOperations.WRITE &&
                                entry.getWriteLockOwner() == xtnEntry && mvccShellEntryCacheInfo.getDirtyEntry().getEntryHolder() == entry) {
                            mvccShellEntryCacheInfo.addEntryGeneration();
                        }
                    } finally {
                        _cacheManager.getLockManager().freeLockObject(entryLock);
                    }
                }
            }
        }
    }

    public IEntryHolder getMatchedEntryAndOperateSA_Entry(Context context,
                                                          ITemplateHolder template,
                                                          boolean makeWaitForInfo,
                                                          MVCCShellEntryCacheInfo shellEntry) throws TemplateDeletedException, TransactionNotActiveException, TransactionConflictException, FifoException, SAException, NoMatchException, EntryDeletedException {
        final XtnEntry xidOriginated = template.getXidOriginated();
        MVCCGenerationsState mvccGenerationsState = null;
        if (xidOriginated != null) {
           mvccGenerationsState = xidOriginated.getMVCCGenerationsState();
        } else {
            // TODO: no transaction - get from context.
            // mvccGenerationsState = context.getMVCCGenerationsState()
        }
        final Iterator<MVCCEntryCacheInfo> generationIterator = shellEntry.descIterator();
        while (generationIterator.hasNext()) {
            final MVCCEntryCacheInfo entryCacheInfo = generationIterator.next();
            final MVCCEntryHolder entryHolder = (MVCCEntryHolder) entryCacheInfo.getEntryHolder();
            final long completedGeneration = mvccGenerationsState.getCompletedGeneration();
            final long overrideGeneration = entryHolder.getOverrideGeneration();
            final long committedGeneration = entryHolder.getCommittedGeneration();
            final boolean logicallyDeleted = entryHolder.isLogicallyDeleted();
            if (
                    (!logicallyDeleted)
                            && (committedGeneration != -1)
                            && (committedGeneration <= completedGeneration)
                            && (!mvccGenerationsState.isUncompletedGeneration(committedGeneration))
                            && ((overrideGeneration == -1)
                            || (overrideGeneration > completedGeneration)
                            || (overrideGeneration <= committedGeneration && mvccGenerationsState.isUncompletedGeneration(overrideGeneration)))
            ) {
                _spaceEngine.performTemplateOnEntrySA(context, template, entryHolder,
                        makeWaitForInfo);
                return entryHolder;
            }
        }
        return null; // continue
    }
}
