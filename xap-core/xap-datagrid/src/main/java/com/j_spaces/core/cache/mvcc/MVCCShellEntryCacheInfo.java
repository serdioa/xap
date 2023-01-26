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

    @Override
    public MVCCShellEntryCacheInfo getMVCCShellEntryCacheInfo() {
        return this;
    }
}
