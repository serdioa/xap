package com.gigaspaces.internal.server.space.mvcc;

import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.space.SpaceUidFactory;
import com.gigaspaces.internal.server.storage.MVCCEntryMetaData;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.cache.mvcc.MVCCEntryCacheInfo;
import com.j_spaces.core.cache.mvcc.MVCCEntryHolder;
import com.j_spaces.core.cache.mvcc.MVCCShellEntryCacheInfo;

import java.util.ArrayList;
import java.util.Iterator;

public class MVCCUtils {
    public static ArrayList<MVCCEntryMetaData> getMVCCEntryMetaData(SpaceEngine engine, String typeName, Object id) {
        Context context = engine.getCacheManager().getCacheContext();

        IServerTypeDesc typeDesc = engine.getTypeManager().getServerTypeDesc(typeName);
        String uid = SpaceUidFactory.createUidFromTypeAndId(typeDesc.getTypeDesc(), id);
        MVCCShellEntryCacheInfo entryCacheInfo = (MVCCShellEntryCacheInfo) engine.getCacheManager().getPEntryByUid(uid);
        Iterator<MVCCEntryCacheInfo> mvccEntryCacheInfoIterator = entryCacheInfo.descIterator();
        ArrayList<MVCCEntryMetaData> metaDataList = new ArrayList<>();
        while(mvccEntryCacheInfoIterator.hasNext()){
            MVCCEntryHolder next = (MVCCEntryHolder) mvccEntryCacheInfoIterator.next().getEntryHolder();
            MVCCEntryMetaData metaData = new MVCCEntryMetaData();
            metaData.setCreatedGeneration(next.getCreatedGeneration());
            metaData.setOverrideGeneration(next.getOverrideGeneration());
            metaData.setLogicallyDeleted(next.isLogicallyDeleted());
            metaData.setOverridingAnother(next.isOverridingAnother());
            metaDataList.add(metaData);
        }
        engine.getCacheManager().freeCacheContext(context);
        return metaDataList;
    }
}
