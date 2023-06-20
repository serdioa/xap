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
package com.gigaspaces.internal.space.requests;

import com.gigaspaces.internal.io.IOUtils;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class GetMVCCEntryMetaDataRequestInfo extends AbstractSpaceRequestInfo{

    private static final long serialVersionUID = -7255029922496067038L;
    private String typeName;
    private Object[] ids;
    private long transactionId;

    public GetMVCCEntryMetaDataRequestInfo(String typeName, Object[] ids, long transactionId) {
        this.typeName = typeName;
        this.ids = ids;
        this.transactionId = transactionId;
    }

    public GetMVCCEntryMetaDataRequestInfo() {
    }

    public String getTypeName() {
        return typeName;
    }

    public Object[] getIds() {
        return ids;
    }


    public long getTransactionId() {
        return transactionId;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeString(out, typeName);
        IOUtils.writeObjectArray(out, ids);
        IOUtils.writeLong(out, transactionId);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        super.readExternal(in);
        this.typeName = IOUtils.readString(in);
        this.ids = IOUtils.readObjectArray(in);
        this.transactionId = IOUtils.readLong(in);
    }
}
