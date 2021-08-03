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
 * Keeps the return type
 *
 * @author Mishel Liberman
 * @since 16.0
 */

public class AverageStrictAggregator extends AverageAggregator {

    private static final long serialVersionUID = 1L;

    public AverageStrictAggregator() {
        super();
    }

    @Override
    public void aggregate(SpaceEntriesAggregatorContext context) {
        Number value = (Number) getPathValue(context);
        if (value != null)
            result = result != null ? result.add(value, 1) : new StrictAverageTuple(value);
    }

    public static class StrictAverageTuple extends AverageTuple {

        private static final long serialVersionUID = 1L;

        private MutableNumber sum;
        private long count;

        public StrictAverageTuple() {
        }

        public StrictAverageTuple(Number sum) {
            this.sum = MutableNumber.fromClass(sum.getClass(), false);
            this.sum.add(sum);
            this.count = 1;
        }

        public AverageAggregator.AverageTuple add(Number deltaSum, long deltaCount) { //TODO: @sagiv why AverageAggregator.AverageTuple?
            this.count += deltaCount;
            this.sum.add(deltaSum);
            return this;
        }

        public Number getAverage() {
            if (count == 0)
                return null;
            return sum.calcAverage(count);
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeLong(count);
            IOUtils.writeObject(out, sum);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            this.count = in.readLong();
            this.sum = IOUtils.readObject(in);
        }
    }

}
