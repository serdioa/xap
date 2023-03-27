package com.j_spaces.core.cache.mvcc;

import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.MemoryBasedEntryCacheInfo;
import com.j_spaces.core.cache.context.Context;

public class MVCCEntryCacheInfo extends MemoryBasedEntryCacheInfo {

    public MVCCEntryCacheInfo(IEntryHolder entryHolder, int backRefsSize) {
        super(entryHolder,backRefsSize);
    }

    public MVCCEntryCacheInfo(IEntryHolder entryHolder) {
        super(entryHolder);
    }

    @Override
    public MVCCEntryHolder getEntryHolder(CacheManager cacheManager) {
        return getEntryHolder();
    }

    @Override
    public MVCCEntryHolder getEntryHolder() {
        return (MVCCEntryHolder) super.getEntryHolder();
    }

    @Override
    public MVCCEntryHolder getEntryHolder(CacheManager cacheManager, Context context) {
        return getEntryHolder();
    }
}
