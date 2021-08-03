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


package com.gigaspaces.query.aggregators;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.serialization.SmartExternalizable;
import com.j_spaces.jdbc.FunctionCallColumn;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;

/**
 * @author Niv Ingberg
 * @since 10.0
 */
public abstract class AbstractPathAggregator<T extends Serializable> extends SpaceEntriesAggregator<T> implements SmartExternalizable {

    private static final long serialVersionUID = 1L;

    private String path;

    private FunctionCallColumn functionCallColumn;

    public String getPath() {
        return functionCallColumn == null ? path : functionCallColumn.getAlias();
    }

    public AbstractPathAggregator setPath(String path) {
        this.path = path;
        return this;
    }

    public FunctionCallColumn getFunctionCallColumn() {
        return functionCallColumn;
    }

    public AbstractPathAggregator setFunctionCallColumn(FunctionCallColumn functionCallColumn) {
        this.functionCallColumn = functionCallColumn;
        return this;
    }

    protected Object getPathValue(SpaceEntriesAggregatorContext context) {
        return functionCallColumn == null ? context.getPathValue(path) : functionCallColumn.apply(context.getPathValue(path));
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeString(out, path);
        final PlatformLogicalVersion version = LRMIInvocationContext.getEndpointLogicalVersion();
        if (version.greaterOrEquals(PlatformLogicalVersion.v16_0_0)) {
            IOUtils.writeObject(out, functionCallColumn);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.path = IOUtils.readString(in);
        final PlatformLogicalVersion version = LRMIInvocationContext.getEndpointLogicalVersion();
        if (version.greaterOrEquals(PlatformLogicalVersion.v16_0_0)) {
            functionCallColumn = IOUtils.readObject(in);
        }
    }
}
