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
