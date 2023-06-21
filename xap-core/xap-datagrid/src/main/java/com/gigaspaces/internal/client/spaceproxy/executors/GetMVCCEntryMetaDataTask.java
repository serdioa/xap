/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
