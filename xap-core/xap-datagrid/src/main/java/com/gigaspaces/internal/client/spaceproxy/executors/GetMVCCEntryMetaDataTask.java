package com.gigaspaces.internal.client.spaceproxy.executors;

import com.gigaspaces.async.AsyncResult;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.space.requests.GetMVCCEntryMetaDataRequestInfo;
import com.gigaspaces.internal.space.requests.SpaceRequestInfo;
import com.gigaspaces.internal.space.responses.GetMVCCEntryMetaDataResponseInfo;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

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
            result.merge(asyncResult.getResult());
        }
        return result;
    }

    @Override
    public SpaceRequestInfo getSpaceRequestInfo() {
        return requestInfo;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeObject(out, requestInfo);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        requestInfo = IOUtils.readObject(in);
    }
}
