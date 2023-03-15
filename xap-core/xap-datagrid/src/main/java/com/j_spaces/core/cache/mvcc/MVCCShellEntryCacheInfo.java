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

import com.gigaspaces.internal.server.storage.EntryHolderFactory;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.j_spaces.core.cache.MemoryBasedEntryCacheInfo;

import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedDeque;

public class MVCCShellEntryCacheInfo extends MemoryBasedEntryCacheInfo {

    private final ConcurrentLinkedDeque<MVCCEntryCacheInfo> allEntryGenerations = new ConcurrentLinkedDeque<>();
    private MVCCEntryCacheInfo dirtyEntry;


    public MVCCShellEntryCacheInfo(IEntryHolder entryHolder, MVCCEntryCacheInfo pEntry) {
        super(EntryHolderFactory.createMvccShellHollowEntry(entryHolder.getServerTypeDesc(), entryHolder.getUID()));
        dirtyEntry = pEntry;
    }

    public Iterator<MVCCEntryCacheInfo> ascIterator(){
        return allEntryGenerations.iterator();
    }

    public Iterator<MVCCEntryCacheInfo> descIterator(){
        return allEntryGenerations.descendingIterator();
    }

    public void addEntryGeneration() {
        allEntryGenerations.add(dirtyEntry);
        dirtyEntry = null;
    }

    public void removeEntryGeneration(MVCCEntryCacheInfo entryCacheInfo) {
        allEntryGenerations.remove(entryCacheInfo); //remove by reference
        assert dirtyEntry == null;
    }

    public MVCCEntryCacheInfo getDirtyEntry() {
        return dirtyEntry;
    }

    public void setDirtyEntry(MVCCEntryCacheInfo dirtyEntry) {
        this.dirtyEntry = dirtyEntry;
    }

    public void clearDirtyEntry() {
        this.dirtyEntry = null;
    }

}
