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
package com.gigaspaces.internal.space.responses;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.server.storage.MVCCEntryMetaData;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GetMVCCEntryMetaDataResponseInfo extends AbstractSpaceResponseInfo{

    private static final long serialVersionUID = 4672636820486293961L;
    private Map<Object, List<MVCCEntryMetaData>> entriesMetaData = new HashMap<>();

    public GetMVCCEntryMetaDataResponseInfo() {
    }

    public Map<Object, List<MVCCEntryMetaData>> getEntriesMetaData() {
        return entriesMetaData;
    }

    public List<MVCCEntryMetaData> getEntriesMetaDataById(Object id) {
        return entriesMetaData.get(id);
    }

    public void addEntryMetaData(Object id, List<MVCCEntryMetaData> entryMetaData) {
        List<MVCCEntryMetaData> mvccEntryMetaData = entriesMetaData.computeIfAbsent(id, (key) -> new ArrayList<>());
        mvccEntryMetaData.addAll(entryMetaData);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeObject(out, this.entriesMetaData);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.entriesMetaData = IOUtils.readObject(in);
    }
}
