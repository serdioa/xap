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

import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.storage.EntryHolderFactory;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.MemoryBasedEntryCacheInfo;
import com.j_spaces.core.cache.context.Context;

import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedDeque;

public class MVCCShellEntryCacheInfo extends MemoryBasedEntryCacheInfo {

    private final ConcurrentLinkedDeque<MVCCEntryCacheInfo> allEntryGenerations = new ConcurrentLinkedDeque<>();
    private volatile MVCCEntryCacheInfo dirtyEntry;
    private final IServerTypeDesc serverTypeDesc;
    private final String uid;


    public MVCCShellEntryCacheInfo(IEntryHolder entryHolder, MVCCEntryCacheInfo pEntry) {
        super(null, pEntry.getBackRefs().size());
        dirtyEntry = pEntry;
        serverTypeDesc = entryHolder.getServerTypeDesc();
        uid = entryHolder.getUID();
    }

    public Iterator<MVCCEntryCacheInfo> ascIterator() {
        return allEntryGenerations.iterator();
    }

    public Iterator<MVCCEntryCacheInfo> descIterator() {
        return allEntryGenerations.descendingIterator();
    }

    public void addDirtyEntryToGenerationQueue() {
        allEntryGenerations.add(dirtyEntry);
        dirtyEntry = null;
    }

    public MVCCEntryCacheInfo getDirtyEntryCacheInfo() {
        return dirtyEntry;
    }

    public MVCCEntryHolder getDirtyEntryHolder() {
        MVCCEntryCacheInfo dirtyEntryCacheInfo = getDirtyEntryCacheInfo();
        if (dirtyEntryCacheInfo != null) {
            return dirtyEntryCacheInfo.getEntryHolder();
        }
        return null;
    }

    public void setDirtyEntryCacheInfo(MVCCEntryCacheInfo dirtyEntry) {
        this.dirtyEntry = dirtyEntry;
    }

    public void clearDirtyEntry() {
        this.dirtyEntry = null;
    }

    public MVCCEntryCacheInfo getLatestGenerationCacheInfo() {
        return allEntryGenerations.peekLast();
    }

    public boolean isLogicallyDeletedOrEmpty() {
        MVCCEntryCacheInfo latestGeneration = getLatestGenerationCacheInfo();
        if (latestGeneration != null) {
            return latestGeneration.getEntryHolder().isLogicallyDeleted();
        }
        return true; //if no generations exist it's same as logically deleted
    }

    @Override
    public MVCCEntryHolder getEntryHolder(CacheManager cacheManager) {
        return getEntryHolder();
    }

    @Override
    public MVCCEntryHolder getEntryHolder() {
        MVCCEntryCacheInfo dirtyEntry = this.dirtyEntry;
        if (dirtyEntry != null) {
            return dirtyEntry.getEntryHolder();
        }
        MVCCEntryCacheInfo latestGeneration = getLatestGenerationCacheInfo();
        if (latestGeneration != null) {
            return latestGeneration.getEntryHolder();
        }
        return EntryHolderFactory.createMvccShellHollowEntry(serverTypeDesc, uid);
    }

    @Override
    public MVCCEntryHolder getEntryHolder(CacheManager cacheManager, Context context) {
        return getEntryHolder();
    }

}
