package com.gigaspaces.internal.server.space.mvcc;

import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.j_spaces.core.*;
import com.j_spaces.core.cache.CacheManager;
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
                            mvccShellEntryCacheInfo.getDirtyEntryHolder().setCommittedGeneration(nextGeneration);
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

    public IEntryHolder getMatchedEntryAndOperateSA_Entry(Context context,
                                                          ITemplateHolder template,
                                                          boolean makeWaitForInfo,
                                                          MVCCShellEntryCacheInfo shellEntry) throws TemplateDeletedException, TransactionNotActiveException, TransactionConflictException, FifoException, SAException, NoMatchException, EntryDeletedException {
        final Iterator<MVCCEntryCacheInfo> generationIterator = shellEntry.descIterator();
        if (!generationIterator.hasNext() && shellEntry.getDirtyEntryCacheInfo() != null){
            return getMvccEntryHolder(context, template, makeWaitForInfo,
                    shellEntry.getDirtyEntryHolder());
        }
        while (generationIterator.hasNext()) {
            final MVCCEntryCacheInfo entryCacheInfo = generationIterator.next();
            final MVCCEntryHolder entryHolder = entryCacheInfo.getEntryHolder();
            final MVCCEntryHolder matchMvccEntryHolder = getMvccEntryHolder(context, template, makeWaitForInfo, entryHolder);
            if (matchMvccEntryHolder != null) return matchMvccEntryHolder;
        }
        return null; // continue
    }


    public IEntryHolder getMatchedEntryAndOperateSA_Entry(Context context,
                                                          ITemplateHolder template,
                                                          boolean makeWaitForInfo,
                                                          MVCCEntryHolder entryHolder) throws TemplateDeletedException,
            TransactionNotActiveException, TransactionConflictException, FifoException, SAException, NoMatchException, EntryDeletedException {
        return getMvccEntryHolder(context, template, makeWaitForInfo, entryHolder);
    }

    private MVCCEntryHolder getMvccEntryHolder(Context context, ITemplateHolder template, boolean makeWaitForInfo,
                                                    MVCCEntryHolder entryHolder) throws TransactionConflictException, EntryDeletedException, TemplateDeletedException, TransactionNotActiveException, SAException, NoMatchException, FifoException {
        if  (entryHolder.isLogicallyDeleted()){
            throw _spaceEngine.getEntryDeletedException();
        }
        _spaceEngine.performTemplateOnEntrySA(context, template, entryHolder, makeWaitForInfo);
        return entryHolder;
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

    public void createLogicallyDeletedEntry(MVCCEntryHolder entryHolder) {
        MVCCShellEntryCacheInfo mvccShellEntryCacheInfo = _cacheManager.getMVCCShellEntryCacheInfoByUid(entryHolder.getUID());
        EntryXtnInfo entryXtnInfo = entryHolder.getTxnEntryData().copyTxnInfo(true, false);
        entryXtnInfo.setWriteLockOperation(SpaceOperations.READ);
        MVCCEntryHolder dummyEntry = entryHolder.createLogicallyDeletedDummyEntry(entryXtnInfo);
        dummyEntry.setMaybeUnderXtn(true);
        mvccShellEntryCacheInfo.setDirtyEntryCacheInfo(new MVCCEntryCacheInfo(dummyEntry));
    }
}
