/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
                        if (entry.getWriteLockOperation() == SpaceOperations.TAKE && entry.getWriteLockOwner() == xtnEntry) {
                            MVCCShellEntryCacheInfo mvccShellEntryCacheInfo = _cacheManager.getMVCCShellEntryCacheInfoByUid(entry.getUID());
                            EntryXtnInfo entryXtnInfo = entry.getTxnEntryData().copyTxnInfo(false, false);
                            MVCCEntryHolder dummyEntry = entry.createLogicallyDeletedDummyEntry(entryXtnInfo);
                            dummyEntry.setMaybeUnderXtn(true);
                            dummyEntry.setCommittedGeneration(nextGeneration);
                            mvccShellEntryCacheInfo.setDirtyEntry(new MVCCEntryCacheInfo(dummyEntry));
                            entry.setOverrideGeneration(nextGeneration);
                            entry.resetEntryXtnInfo();
                            entry.setMaybeUnderXtn(true);
                        } else if (entry.getWriteLockOperation() == SpaceOperations.WRITE && entry.getWriteLockOwner() == xtnEntry) {
                            entry.setCommittedGeneration(nextGeneration);
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
        final MVCCGenerationsState mvccGenerationsState = getMvccGenerationsState(context, template);
        final Iterator<MVCCEntryCacheInfo> generationIterator = shellEntry.descIterator();
        if (!generationIterator.hasNext() && shellEntry.getDirtyEntry() != null){
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
        }
        // TODO: no transaction - get from context/template
        return mvccGenerationsState;
    }

    private MVCCEntryHolder getMatchMvccEntryHolder(Context context, ITemplateHolder template, boolean makeWaitForInfo,
                                                    MVCCEntryHolder entryHolder, MVCCGenerationsState mvccGenerationsState) throws TransactionConflictException, EntryDeletedException, TemplateDeletedException, TransactionNotActiveException, SAException, NoMatchException, FifoException {

        if (template.isActiveRead(_spaceEngine)
                || isEntryMatchedByGenerationsState(mvccGenerationsState, entryHolder)
                || entryHolder.getCommittedGeneration() == -1 /*dirty entry*/) {
            if  (entryHolder.isLogicallyDeleted()){
                throw _spaceEngine.getEntryDeletedException();
            }
            _spaceEngine.performTemplateOnEntrySA(context, template, entryHolder, makeWaitForInfo);
            return entryHolder;
        }
        return null; // continue
    }

    private boolean isEntryMatchedByGenerationsState(MVCCGenerationsState mvccGenerationsState, MVCCEntryHolder entryHolder) {
        final long completedGeneration = mvccGenerationsState.getCompletedGeneration();
        final long overrideGeneration = entryHolder.getOverrideGeneration();
        final long committedGeneration = entryHolder.getCommittedGeneration();
        return ((committedGeneration != -1)
                && (committedGeneration <= completedGeneration)
                && (!mvccGenerationsState.isUncompletedGeneration(committedGeneration))
                && ((overrideGeneration == -1)
                    || (overrideGeneration > completedGeneration)
                    || (overrideGeneration <= committedGeneration && mvccGenerationsState.isUncompletedGeneration(overrideGeneration))));
    }
}
