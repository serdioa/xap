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
