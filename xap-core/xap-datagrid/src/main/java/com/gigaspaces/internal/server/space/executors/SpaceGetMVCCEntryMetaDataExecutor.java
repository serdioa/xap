package com.gigaspaces.internal.server.space.executors;

import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.space.requests.GetMVCCEntryMetaDataRequestInfo;
import com.gigaspaces.internal.space.requests.SpaceRequestInfo;
import com.gigaspaces.internal.space.responses.GetMVCCEntryMetaDataResponseInfo;
import com.gigaspaces.internal.space.responses.SpaceResponseInfo;

public class SpaceGetMVCCEntryMetaDataExecutor extends SpaceActionExecutor{

    @Override
    public SpaceResponseInfo execute(SpaceImpl space, SpaceRequestInfo spaceRequestInfo) {
        GetMVCCEntryMetaDataResponseInfo response = new GetMVCCEntryMetaDataResponseInfo();
        GetMVCCEntryMetaDataRequestInfo request = (GetMVCCEntryMetaDataRequestInfo) spaceRequestInfo;
        for (Object id : request.getIds()) {
            response.addEntryMetaData(id, space.getMVCCEntryMetaData(request.getTypeName(), id));
//            if (space.isMVCCEntryDirtyUnderTransaction(request.getTypeName(), id, request.getTransactionId())) {
//                response.setIsDirty(id);
//            }
        }
        return response;
    }
}
