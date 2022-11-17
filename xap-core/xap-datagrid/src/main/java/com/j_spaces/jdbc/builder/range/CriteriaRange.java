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
package com.j_spaces.jdbc.builder.range;

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.storage.IEntryData;
import com.j_spaces.core.client.SQLQuery;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

@com.gigaspaces.api.InternalApi
public class CriteriaRange extends Range {
    // serialVersionUID should never be changed.
    private static final long serialVersionUID = 1L;


    // true when OR is used, false when AND is used
    private final boolean isUnion;
    // list of ranges that will be used to filter the results
    private final List<Range> _ranges = new ArrayList<>();

    public CriteriaRange(boolean isUnion) {
        this.isUnion = isUnion;
    }


    public boolean isUnion() {
        return isUnion;
    }

    public List<Range> getRanges() {
        return _ranges;
    }

    /**
     * Add a range to the composite ranges
     */
    public CriteriaRange add(Range range) {
        _ranges.add(range);
        return this;
    }

    @Override
    public Range intersection(Range range) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Range intersection(SegmentRange range) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Range intersection(EqualValueRange range) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Range intersection(NotEqualValueRange range) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Range intersection(NotNullRange range) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Range intersection(IsNullRange range) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void toEntryPacket(QueryTemplatePacket e, int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Range intersection(RegexRange range) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Range intersection(RelationRange range) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Range intersection(NotRegexRange range) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Range intersection(InRange range) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

    public SQLQuery toSQLQuery(ITypeDesc typeDesc) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean evaluatePredicate(IEntryData entryData) {
        boolean result = false;
        for (int i = 0; i < getRanges().size(); i++) {
            Range range = getRanges().get(i);
            result = range.evaluatePredicate(entryData);
            if (!result && !isUnion())
                return false;
            if (result && isUnion())
                return true;

        }
        return result;
    }
}
