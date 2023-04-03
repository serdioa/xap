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
package com.gigaspaces.internal.transport;


import com.gigaspaces.entry.CompoundSpaceId;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class CompoundRoutingHashValue implements SmartExternalizable {

    private static final long serialVersionUID = -6784381691438604333L;
    int hash = 0;

    public CompoundRoutingHashValue() {

    }

    /**
     * compute hash according to hashcode of {@link CompoundSpaceId#hashCode()} which uses Arrays.hashCode()
     */
    public void concatValue(Object routingValue) {
        hash = 31 * hash + routingValue.hashCode();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeInt(out, hash);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.hash = IOUtils.readInt(in);
    }


    @Override
    public int hashCode() {
        return hash;
    }
}