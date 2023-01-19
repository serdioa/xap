package com.j_spaces.core.cache.mvcc;

import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.j_spaces.core.cache.MemoryBasedEntryCacheInfo;

public class MVCCEntryCacheInfo extends MemoryBasedEntryCacheInfo {

    public MVCCEntryCacheInfo(IEntryHolder entryHolder, int backRefsSize) {
        super(entryHolder,backRefsSize);
    }

    public MVCCEntryCacheInfo(IEntryHolder entryHolder) {
        super(entryHolder);
    }

}
