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
package com.gigaspaces.internal.server.space.executors;

import com.gigaspaces.internal.client.spaceproxy.executors.SystemTask;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.space.requests.GSMessageRequestInfo;
import com.gigaspaces.internal.space.requests.SpaceRequestInfo;
import com.gigaspaces.internal.space.responses.SpaceResponseInfo;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


public class GSMessageTask extends SystemTask<SpaceResponseInfo> {

    private final static long serialVersionUID = -7309198720798429132L;
    private GSMessageRequestInfo requestInfo;

    public GSMessageTask() {
    }

    public GSMessageTask(GSMessageRequestInfo requestInfo) {
        this.requestInfo = requestInfo;
    }

    public enum OperationType {INSERT, UPDATE, DELETE}

    @Override
    public boolean enabledSmartExternalizableWithReference() {
        return super.enabledSmartExternalizableWithReference();
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
        this.requestInfo = IOUtils.readObject(in);
    }

}
