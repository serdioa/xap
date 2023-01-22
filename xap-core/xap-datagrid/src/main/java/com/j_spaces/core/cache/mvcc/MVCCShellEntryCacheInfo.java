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

import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.j_spaces.core.cache.MemoryBasedEntryCacheInfo;

import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedDeque;

public class MVCCShellEntryCacheInfo extends MemoryBasedEntryCacheInfo {

    private final ConcurrentLinkedDeque<MemoryBasedEntryCacheInfo> allEntryGenerations = new ConcurrentLinkedDeque<>();

    public MVCCShellEntryCacheInfo(IEntryHolder entryHolder) {
        super(entryHolder);
    }

    public MVCCShellEntryCacheInfo(IEntryHolder entryHolder, int backRefsSize) {
        super(entryHolder, backRefsSize);
    }

    public Iterator<MemoryBasedEntryCacheInfo> ascIterator(){
        return allEntryGenerations.iterator();
    }

    public Iterator<MemoryBasedEntryCacheInfo> descIterator(){
        return allEntryGenerations.descendingIterator();
    }

    public void addEntryGeneration(MemoryBasedEntryCacheInfo entryCacheInfo) {
        allEntryGenerations.add(entryCacheInfo);
    }

    public void removeEntryGeneration(MemoryBasedEntryCacheInfo entryCacheInfo) {
        allEntryGenerations.remove(entryCacheInfo); //remove by reference
    }
}
