package com.gigaspaces.internal.server.space.mvcc;

import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.space.SpaceUidFactory;
import com.gigaspaces.internal.server.storage.MVCCEntryMetaData;
import com.j_spaces.core.XtnEntry;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.cache.mvcc.MVCCEntryCacheInfo;
import com.j_spaces.core.cache.mvcc.MVCCEntryHolder;
import com.j_spaces.core.cache.mvcc.MVCCShellEntryCacheInfo;

import java.util.ArrayList;
import java.util.Iterator;

public class MVCCUtils {
    public static ArrayList<MVCCEntryMetaData> getMVCCEntryMetaData(SpaceEngine engine, String typeName, Object id) {
        Context context = engine.getCacheManager().getCacheContext();
        ArrayList<MVCCEntryMetaData> metaDataList = new ArrayList<>();

        IServerTypeDesc typeDesc = engine.getTypeManager().getServerTypeDesc(typeName);
        String uid = SpaceUidFactory.createUidFromTypeAndId(typeDesc.getTypeDesc(), id);

        MVCCShellEntryCacheInfo mvccShellEntryCacheInfo = engine.getCacheManager().getMVCCShellEntryCacheInfoByUid(uid);
        Iterator<MVCCEntryCacheInfo> mvccEntryCacheInfoIterator = mvccShellEntryCacheInfo.descIterator();
        while(mvccEntryCacheInfoIterator.hasNext()){
            MVCCEntryHolder next = mvccEntryCacheInfoIterator.next().getEntryHolder();
            MVCCEntryMetaData metaData = new MVCCEntryMetaData();
            metaData.setCommittedGeneration(next.getCommittedGeneration());
            metaData.setOverrideGeneration(next.getOverrideGeneration());
            metaData.setLogicallyDeleted(next.isLogicallyDeleted());
            metaData.setOverridingAnother(next.isOverridingAnother());
            metaDataList.add(metaData);
        }
        engine.getCacheManager().freeCacheContext(context);
        return metaDataList;
    }

    public static MVCCEntryMetaData getMVCCDirtyEntryUnderTransaction(SpaceEngine engine, String typeName, Object id,
                                                                      long transactionId) {
        if (transactionId == -1) return null;
        IServerTypeDesc typeDesc = engine.getTypeManager().getServerTypeDesc(typeName);
        String uid = SpaceUidFactory.createUidFromTypeAndId(typeDesc.getTypeDesc(), id);
        MVCCShellEntryCacheInfo mvccShellEntryCacheInfo = engine.getCacheManager().getMVCCShellEntryCacheInfoByUid(uid);

        MVCCEntryCacheInfo dirtyEntry = null;
        long dirtyTransactionId = -1;
        if (mvccShellEntryCacheInfo != null) {
            dirtyEntry = mvccShellEntryCacheInfo.getDirtyEntryCacheInfo();
            XtnEntry xidOriginated = dirtyEntry != null ? dirtyEntry.getEntryHolder().getXidOriginated() : null;
            dirtyTransactionId = xidOriginated != null ? xidOriginated.m_Transaction.id : -1;
        }
        boolean isDirtyUnderTransaction = dirtyTransactionId != -1 && dirtyTransactionId == transactionId;
        return isDirtyUnderTransaction ? mvccEntryHolderToMVCCEntryMetaData(dirtyEntry.getEntryHolder()) : null;
    }

    private static MVCCEntryMetaData mvccEntryHolderToMVCCEntryMetaData(MVCCEntryHolder entryHolder) {
        if (entryHolder == null) {
            return null;
        }
        MVCCEntryMetaData mvccDirtyEntryMetaData = new MVCCEntryMetaData();
        mvccDirtyEntryMetaData.setCommittedGeneration(entryHolder.getCommittedGeneration());
        mvccDirtyEntryMetaData.setOverrideGeneration(entryHolder.getOverrideGeneration());
        mvccDirtyEntryMetaData.setLogicallyDeleted(entryHolder.isLogicallyDeleted());
        mvccDirtyEntryMetaData.setOverridingAnother(entryHolder.isOverridingAnother());
        return mvccDirtyEntryMetaData;
    }
}
