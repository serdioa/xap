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

    private static final long serialVersionUID = -8600456640392621464L;
    private String typeName;
    private Object[] ids;

    public GetMVCCEntryMetaDataRequestInfo(String typeName, Object[] ids) {
        this.typeName = typeName;
        this.ids = ids;
    }

    public GetMVCCEntryMetaDataRequestInfo() {
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public Object[] getIds() {
        return ids;
    }

    public void setId(Object[] ids) {
        this.ids = ids;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeString(out, typeName);
        IOUtils.writeObjectArray(out, ids);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        super.readExternal(in);
        this.typeName = IOUtils.readString(in);
        this.ids = IOUtils.readObjectArray(in);
    }
}
