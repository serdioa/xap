package com.j_spaces.core.cache.mvcc;

import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.space.mvcc.MVCCGenerationsState;
import com.gigaspaces.internal.server.storage.EntryHolderFactory;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.IEntryCacheInfo;
import com.j_spaces.core.cache.MemoryBasedEntryCacheInfo;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.sadapter.SAException;
import com.j_spaces.kernel.list.IScanListIterator;

import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedDeque;

public class MVCCShellEntryCacheInfo extends MemoryBasedEntryCacheInfo {

    private final ConcurrentLinkedDeque<MVCCEntryCacheInfo> allEntryGenerations = new ConcurrentLinkedDeque<>();
    private volatile MVCCEntryCacheInfo dirtyEntry;
    private final IServerTypeDesc serverTypeDesc;
    private final String uid;
    private final Object id;


    public MVCCShellEntryCacheInfo(IEntryHolder entryHolder, MVCCEntryCacheInfo pEntry) {
        super(null, pEntry.getBackRefs().size());
        dirtyEntry = pEntry;
        serverTypeDesc = entryHolder.getServerTypeDesc();
        uid = entryHolder.getUID();
        id = entryHolder.getEntryId();
    }


    public Object getShellID() {
        return id;
    }

    @Override
    public IServerTypeDesc getServerTypeDesc() {
        return serverTypeDesc;
    }

    @Override
    public String getUID() {
        return uid;
    }

    public Iterator<MVCCEntryCacheInfo> ascIterator() {
        return allEntryGenerations.iterator();
    }

    public Iterator<MVCCEntryCacheInfo> descIterator() {
        return allEntryGenerations.descendingIterator();
    }

    public void addCommittedEntryToGenerationQueue(MVCCEntryCacheInfo committedEntry) {
        allEntryGenerations.add(committedEntry);
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

    public MVCCEntryCacheInfo getOldestGenerationCacheInfo() {
        return allEntryGenerations.peekFirst();
    }

    public MVCCEntryCacheInfo removeOldestGenerationCacheInfo() {
        return allEntryGenerations.removeFirst();
    }

    public MVCCEntryHolder getLatestCommittedOrHollow() {
        MVCCEntryCacheInfo latestGeneration = getLatestGenerationCacheInfo();
        if (latestGeneration != null) {
            return latestGeneration.getEntryHolder();
        }
        return EntryHolderFactory.createMvccShellHollowEntry(serverTypeDesc, uid);
    }

    // Used under lock when running revert task. Should use it with the uncompleted entry only.
    public boolean removeUncompletedEntryFromQueue(MVCCEntryHolder revertedEntry, MVCCGenerationsState generationsState) {
        Iterator<MVCCEntryCacheInfo> mvccEntryCacheInfoIterator = descIterator();
        while (mvccEntryCacheInfoIterator.hasNext()) {
            MVCCEntryHolder entryHolder = mvccEntryCacheInfoIterator.next().getEntryHolder();
            if (entryHolder.equals(revertedEntry) && generationsState.isUncompletedGeneration(entryHolder.getCommittedGeneration())) {
                mvccEntryCacheInfoIterator.remove();
                return true;
            }
        }
        return false;
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
        return getLatestCommittedOrHollow();
    }

    @Override
    public MVCCEntryHolder getEntryHolder(CacheManager cacheManager, Context context) {
        return getEntryHolder();
    }

    @Override
    public boolean hasNext() throws SAException {
        return false;
    }

    @Override
    public IEntryCacheInfo next() throws SAException {
        return null;
    }

    @Override
    public boolean isIterator() {
        return true;
    }

    public int getTotalCommittedGenertions() {
        return allEntryGenerations.size();
    }

    public boolean isLogicallyDeleted() {
        return this.getLatestGenerationCacheInfo() != null && this.getLatestGenerationCacheInfo().getEntryHolder().isLogicallyDeleted();
    }

    public boolean isEmptyShell() {
        return getTotalCommittedGenertions() == 0 && getDirtyEntryCacheInfo() == null;
    }

    @Override
    public IScanListIterator<MVCCEntryCacheInfo> createCopyForAlternatingThread() {
        return new MVCCShellIterator();
    }

    private class MVCCShellIterator implements IScanListIterator<MVCCEntryCacheInfo>, Iterator<MVCCEntryCacheInfo> {
        private MVCCEntryCacheInfo dirty = getDirtyEntryCacheInfo();
        private Iterator<MVCCEntryCacheInfo> descIterator = descIterator();

        @Override
        public boolean hasNext() {
            return dirty != null || descIterator.hasNext();
        }

        @Override
        public MVCCEntryCacheInfo next() {
            if (dirty != null) {
                final MVCCEntryCacheInfo toReturn = dirty;
                dirty = null;
                return toReturn;
            }
            return descIterator.next();
        }

        @Override
        public void releaseScan() {
            if (dirty != null) {
                dirty = null;
            }
            if (descIterator != null) {
                descIterator = null;
            }
        }

        @Override
        public int getAlreadyMatchedFixedPropertyIndexPos() {
            return -1;
        }

        @Override
        public boolean isAlreadyMatched() {
            return false;
        }

        @Override
        public boolean isIterator() {
            return true;
        }
    }
}
