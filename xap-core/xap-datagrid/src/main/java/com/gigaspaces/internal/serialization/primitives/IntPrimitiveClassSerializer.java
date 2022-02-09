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
package com.gigaspaces.internal.serialization.primitives;

import com.gigaspaces.internal.serialization.IClassSerializer;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

@com.gigaspaces.api.InternalApi
public class IntPrimitiveClassSerializer implements IClassSerializer<Integer> {
    private static final Integer DEFAULT_VALUE = 0;

    public static final IntPrimitiveClassSerializer instance = new IntPrimitiveClassSerializer();

    private IntPrimitiveClassSerializer() {
    }

    public byte getCode() {
        return CODE_INTEGER;
    }

    public Integer read(ObjectInput in)
            throws IOException, ClassNotFoundException {
        return in.readInt();
    }

    public void write(ObjectOutput out, Integer obj)
            throws IOException {
        out.writeInt(obj);
    }

    @Override
    public Integer getDefaultValue() {
        return DEFAULT_VALUE;
    }
}
