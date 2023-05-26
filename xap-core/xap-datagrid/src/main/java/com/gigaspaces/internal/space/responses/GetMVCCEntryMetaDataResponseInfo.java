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
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class GetMVCCEntryMetaDataResponseInfo extends AbstractSpaceResponseInfo{

    private static final long serialVersionUID = 4672636820486293961L;
    private Map<Object, List<MVCCEntryMetaData>> entriesMetaData = new HashMap<>();

    private Map<Object, MVCCEntryMetaData> dirtyMetaData = new HashMap<>();

    public GetMVCCEntryMetaDataResponseInfo() {
    }

    public List<MVCCEntryMetaData> getEntriesMetaDataById(Object id) {
        return entriesMetaData.get(id);
    }

    public void addEntryMetaData(Object id, List<MVCCEntryMetaData> entryMetaData) {
        List<MVCCEntryMetaData> mvccEntryMetaData = entriesMetaData.computeIfAbsent(id, (key) -> new ArrayList<>());
        mvccEntryMetaData.addAll(entryMetaData);
    }

    public MVCCEntryMetaData getDirtyMetaDataById(Object id) {
        return dirtyMetaData.get(id);
    }

    public void addDirtyMetaData(Object id, MVCCEntryMetaData dirtyMetaData) {
        this.dirtyMetaData.put(id, dirtyMetaData);
    }

    public Boolean isDirty(Object id) {
        return dirtyMetaData.containsKey(id);
    }

    public void merge(GetMVCCEntryMetaDataResponseInfo other) {
        this.entriesMetaData = Stream.of(this.entriesMetaData, other.entriesMetaData)
                .flatMap(map -> map.entrySet().stream())
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (mergeValue, otherValue) -> {
                            mergeValue.addAll(otherValue);
                            return mergeValue;
                        }));

        this.dirtyMetaData = Stream.of(this.dirtyMetaData, other.dirtyMetaData)
                .flatMap(map -> map.entrySet().stream())
                .filter(e -> e.getValue() != null)
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue
                ));
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeObject(out, this.entriesMetaData);
        IOUtils.writeObject(out, this.dirtyMetaData);

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this.entriesMetaData = IOUtils.readObject(in);
        this.dirtyMetaData = IOUtils.readObject(in);
    }
}
