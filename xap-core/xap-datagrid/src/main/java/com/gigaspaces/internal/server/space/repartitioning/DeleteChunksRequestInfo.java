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

import com.gigaspaces.admin.quiesce.QuiesceToken;
import com.gigaspaces.internal.cluster.ClusterTopology;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.space.requests.SpaceRequestInfo;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.j_spaces.core.SpaceContext;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class DeleteChunksRequestInfo implements SpaceRequestInfo {
    private static final long serialVersionUID = 4826314985077083352L;
    private SpaceContext context;
    private ClusterTopology newMap;
    private QuiesceToken token;

    DeleteChunksRequestInfo(ClusterTopology newMap, QuiesceToken token) {
        this.newMap = newMap;
        this.token = token;
    }

    public DeleteChunksRequestInfo() {
    }

    public ClusterTopology getNewMap() {
        return newMap;
    }

    public QuiesceToken getToken() {
        return token;
    }

    @Override
    public SpaceContext getSpaceContext() {
        return this.context;
    }

    @Override
    public void setSpaceContext(SpaceContext spaceContext) {
        this.context = spaceContext;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeObject(out, newMap);
        IOUtils.writeObject(out, token);
        if (LRMIInvocationContext.getEndpointLogicalVersion().greaterThan(PlatformLogicalVersion.v16_4_0)) {
            IOUtils.writeObject(out, context);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.newMap = IOUtils.readObject(in);
        this.token = IOUtils.readObject(in);
        if (LRMIInvocationContext.getEndpointLogicalVersion().greaterThan(PlatformLogicalVersion.v16_4_0)) {
            this.context = IOUtils.readObject(in);
        }
    }
}
