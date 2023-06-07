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

    public MVCCEntryHolder getMVCCEntryIfMatched(ITemplateHolder template, MVCCEntryCacheInfo entryCacheInfo) {
        MVCCEntryHolder entryHolder = entryCacheInfo.getEntryHolder();
        if (entryHolder.isLogicallyDeleted() || !isEntryMatchedByGenerationsState(entryHolder, template)) {
            return null; // continue
        }
        return entryHolder;
    }

    private boolean isEntryMatchedByGenerationsState(MVCCEntryHolder entryHolder, ITemplateHolder template) {
        final MVCCGenerationsState mvccGenerationsState = template.getGenerationsState();
        final long completedGeneration = mvccGenerationsState == null ? -1 : mvccGenerationsState.getCompletedGeneration();
        final long overrideGeneration = entryHolder.getOverrideGeneration();
        final long committedGeneration = entryHolder.getCommittedGeneration();
        final boolean isDirtyEntry = committedGeneration == -1 && overrideGeneration == -1;
        if (template.isReadOperation()) {
            if (template.isActiveRead(_spaceEngine)){
                return committedGeneration == -1 || overrideGeneration == -1;
            } else{
                return isDirtyEntry
                        || ((committedGeneration != -1)
                        && (committedGeneration <= completedGeneration)
                        && (!mvccGenerationsState.isUncompletedGeneration(committedGeneration))
                        && ((overrideGeneration == -1)
                        || (overrideGeneration > completedGeneration)
                        || (overrideGeneration <= completedGeneration && mvccGenerationsState.isUncompletedGeneration(overrideGeneration))));
            }
        } else {
            return isDirtyEntry
                    || ((committedGeneration != -1)
                    && (committedGeneration <= completedGeneration)
                    && (!mvccGenerationsState.isUncompletedGeneration(committedGeneration))
                    && (overrideGeneration == -1));
        }
    }

    public SpaceEngine.XtnConflictCheckIndicators checkTransactionConflict(Context context, MVCCEntryHolder entry, ITemplateHolder template) {
        int templateOperation = template.getTemplateOperation();
        switch (templateOperation) {
            case SpaceOperations.TAKE_IE:
            case SpaceOperations.TAKE:
                if (entry.isLogicallyDeleted()) {
                    if (_spaceEngine.getLogger().isDebugEnabled()) {
                        _spaceEngine.getLogger().debug("Encountered a conflict while attempting to modify " + entry
                                + ", this entry is logically deleted."
                                + " the current generation state is " + template.getGenerationsState());
                    }
                    return SpaceEngine.XtnConflictCheckIndicators.ENTRY_DELETED;
                }
            case SpaceOperations.UPDATE:
                if (entry.getOverrideGeneration() > -1) {
                    if (_spaceEngine.getLogger().isDebugEnabled()) {
                        _spaceEngine.getLogger().debug("Encountered a conflict while attempting to modify " + entry
                                + ", this entry has already overridden by another generation."
                                + " the current generation state is " + template.getGenerationsState());
                    }
                    return SpaceEngine.XtnConflictCheckIndicators.XTN_CONFLICT;
                } else{
                    if (entry.isLogicallyDeleted() && entry.getCommittedGeneration() > -1 /* active entry logically deleted*/
                            && !template.getGenerationsState().isUncompletedGeneration(entry.getCommittedGeneration())){
                        if (_spaceEngine.getLogger().isDebugEnabled()) {
                            _spaceEngine.getLogger().debug("Encountered a conflict while attempting to modify " + entry
                                    + ", this entry is logically deleted."
                                    + " the current generation state is " + template.getGenerationsState());
                        }
                        return SpaceEngine.XtnConflictCheckIndicators.ENTRY_DELETED;
                    }
                }
        }
        return SpaceEngine.XtnConflictCheckIndicators.NO_CONFLICT;
    }


}
