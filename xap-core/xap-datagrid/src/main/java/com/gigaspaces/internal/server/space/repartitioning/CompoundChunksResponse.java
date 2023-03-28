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
package com.gigaspaces.internal.server.space.repartitioning;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.space.responses.SpaceResponseInfo;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

public class CompoundChunksResponse implements SpaceResponseInfo {
    private static final long serialVersionUID = -1679935942453681969L;
    private List<SpaceResponseInfo> responses;

    public CompoundChunksResponse() {
    }

    void addResponse(SpaceResponseInfo responseInfo) {
        if (this.responses == null) {
            this.responses = new ArrayList<>();
        }
        this.responses.add(responseInfo);
    }

    public List<SpaceResponseInfo> getResponses() {
        return responses;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeInt(out, responses.size());
        for (SpaceResponseInfo response : responses) {
            IOUtils.writeObject(out, response);
        }

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int size = IOUtils.readInt(in);
        this.responses = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            responses.add(IOUtils.readObject(in));
        }

    }
}
