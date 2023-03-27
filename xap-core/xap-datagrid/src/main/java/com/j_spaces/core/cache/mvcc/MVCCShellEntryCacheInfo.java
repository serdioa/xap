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

    public MVCCEntryCacheInfo getLatestGeneration(){
        return allEntryGenerations.peekLast();
    }

    public boolean isLogicallyDeletedOrEmpty() {
        MVCCEntryCacheInfo latestGeneration = getLatestGeneration();
        if (latestGeneration != null){
            return latestGeneration.getMVCCEntryHolder().isLogicallyDeleted();
        }
        return true; //if no generations exist it's same as logically deleted
    }

    @Override
    public IEntryHolder getEntryHolder(CacheManager cacheManager) {
        return getEntryHolder();
    }

    @Override
    public IEntryHolder getEntryHolder() {
        MVCCEntryCacheInfo latestGeneration = getLatestGeneration();
        if (latestGeneration != null){
            return latestGeneration.getEntryHolder();
        }
        if(dirtyEntry != null){
            return dirtyEntry.getEntryHolder();
        }
        return EntryHolderFactory.createMvccShellHollowEntry(serverTypeDesc, uid);
    }

    @Override
    public IEntryHolder getEntryHolder(CacheManager cacheManager, Context context) {
        return getEntryHolder();
    }
}
