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
import com.gigaspaces.internal.utils.math.MutableNumber;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Sagiv Michael
 * @since 16.0
 */

public class SumZeroAggregator extends AbstractPathAggregator<MutableNumber> {

    private static final long serialVersionUID = 1L;

    private transient MutableNumber result;

    private Number value;

    private Class<?> type;

    public SumZeroAggregator() {
    }

    public SumZeroAggregator(Class<?> type) {
        this.type = type;
    }

    public Number getValue() {
        return value;
    }

    public SumZeroAggregator setValue(Number value) {
        this.value = value;
        return this;
    }


    @Override
    public String getDefaultAlias() {
        return "sum0(" + getPath() + ")";
    }

    @Override
    public void aggregate(SpaceEntriesAggregatorContext context) {
        if (value == null) {
            add((Number) getPathValue(context));
        } else {
            add(value);
        }
    }

    private void add(Number number) {
        if (number != null) {
            if (result == null)
                result = MutableNumber.fromClass(number.getClass(), false);
            result.add(number);
        }
    }

    @Override
    public MutableNumber getIntermediateResult() {
        return result;
    }

    @Override
    public void aggregateIntermediateResult(MutableNumber partitionResult) {
        if (result == null) {
            result = partitionResult;
        }
        else {
            result.add(partitionResult.toNumber());
        }
    }

    @Override
    public Object getFinalResult() {
        return result != null ? result.toNumber() : MutableNumber.fromClass(type, false).toNumber();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeObject(out, value);
        IOUtils.writeObject(out, type);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        value = IOUtils.readObject(in);
        type = IOUtils.readObject(in);
    }
}
