package com.gigaspaces.internal.client.spaceproxy.executors;

import com.gigaspaces.async.AsyncResult;
import com.gigaspaces.internal.server.storage.MVCCEntryMetaData;
import com.gigaspaces.internal.space.requests.GetMVCCEntryMetaDataRequestInfo;
import com.gigaspaces.internal.space.requests.SpaceRequestInfo;
import com.gigaspaces.internal.space.responses.GetMVCCEntryMetaDataResponseInfo;

import java.util.List;
import java.util.Map;

@com.gigaspaces.api.InternalApi
public class GetMVCCEntryMetaDataTask extends SystemDistributedTask<GetMVCCEntryMetaDataResponseInfo>{

    private static final long serialVersionUID = 4211169160769386203L;

    private GetMVCCEntryMetaDataRequestInfo requestInfo;

    public GetMVCCEntryMetaDataTask() {
    }

    public GetMVCCEntryMetaDataTask(GetMVCCEntryMetaDataRequestInfo requestInfo) {
        this.requestInfo = requestInfo;
    }

    @Override
    public GetMVCCEntryMetaDataResponseInfo reduce(List<AsyncResult<GetMVCCEntryMetaDataResponseInfo>> asyncResults) throws Exception {
        GetMVCCEntryMetaDataResponseInfo result = new GetMVCCEntryMetaDataResponseInfo();
        for (AsyncResult<GetMVCCEntryMetaDataResponseInfo> asyncResult : asyncResults){
            if (asyncResult.getException() != null) {
                throw asyncResult.getException();
            }
            GetMVCCEntryMetaDataResponseInfo responseInfo = asyncResult.getResult();
            for(Map.Entry<Object, List<MVCCEntryMetaData>> entry: responseInfo.getEntriesMetaData().entrySet()) {
                result.addEntryMetaData(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }

    @Override
    public SpaceRequestInfo getSpaceRequestInfo() {
        return requestInfo;
    }
}
