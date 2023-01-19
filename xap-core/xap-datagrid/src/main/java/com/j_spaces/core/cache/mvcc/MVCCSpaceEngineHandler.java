package com.j_spaces.core.cache.mvcc;

import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.space.mvcc.MVCCGenerationsState;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.j_spaces.core.XtnEntry;
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

    public void commitMVCCEntries(Context context, XtnEntry xtnEntry) {
        MVCCGenerationsState mvccGenerationsState = xtnEntry.getMVCCGenerationsState();
        ISAdapterIterator<IEntryHolder> entriesIter = null;
        try {
            entriesIter = _cacheManager.makeUnderXtnEntriesIter(context,
                    xtnEntry, SelectType.ALL_ENTRIES, false /* returnPEntry*/);
            if (entriesIter != null) {
                while (true) {
                    MVCCEntryHolder entry = (MVCCEntryHolder) entriesIter.next();
                    if (entry == null)
                        break;
                    MVCCShellEntryCacheInfo mvccShellEntryCacheInfo = (MVCCShellEntryCacheInfo) _cacheManager.getPEntryByUid(entry.getUID());
                    long nextGeneration = mvccGenerationsState.getNextGeneration();
                    entry.setCreatedGeneration(nextGeneration);
                    Iterator<MVCCEntryCacheInfo> shellIterator = mvccShellEntryCacheInfo.descIterator();
                    if (shellIterator.hasNext()){
                        MVCCEntryCacheInfo overrideEntry = shellIterator.next();
                        entry.setOverridingAnother(true);
                        ((MVCCEntryHolder)overrideEntry.getEntryHolder()).setOverrideGeneration(nextGeneration);
                    }
                    mvccShellEntryCacheInfo.addEntryGeneration();
                }
            }
        } catch (SAException e) {
            throw new RuntimeException(e);
        }

    }
}
