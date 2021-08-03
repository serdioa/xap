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

import com.gigaspaces.internal.utils.math.MutableNumber;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Niv Ingberg
 * @since 10.0
 */

public class SumAggregator extends AbstractPathAggregator<MutableNumber> {

    private static final long serialVersionUID = 1L;

    protected transient MutableNumber result;
    private boolean widest = true;

    public SumAggregator() {
    }

    @Override
    public String getDefaultAlias() {
        return "sum(" + getPath() + ")";
    }

    @Override
    public void aggregate(SpaceEntriesAggregatorContext context) {
        add((Number) getPathValue(context));
    }

    @Override
    public MutableNumber getIntermediateResult() {
        return result;
    }

    @Override
    public void aggregateIntermediateResult(MutableNumber partitionResult) {
        if (result == null)
            result = partitionResult;
        else
            result.add(partitionResult.toNumber());
    }

    @Override
    public Object getFinalResult() {
        return result != null ? result.toNumber() : null;
    }

    protected void add(Number number) {
        if (number != null) {
            if (result == null)
                result = MutableNumber.fromClass(number.getClass(), widest);
            result.add(number);
        }
    }

    public SumAggregator setWidest(boolean widest) {
        this.widest = widest;
        return this;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        PlatformLogicalVersion logicalVersion = LRMIInvocationContext.getEndpointLogicalVersion();
        if(logicalVersion.greaterOrEquals(PlatformLogicalVersion.v16_0_0)){
            out.writeBoolean(widest);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        PlatformLogicalVersion logicalVersion = LRMIInvocationContext.getEndpointLogicalVersion();
        if(logicalVersion.greaterOrEquals(PlatformLogicalVersion.v16_0_0)){
            widest = in.readBoolean();
        }
    }
}
