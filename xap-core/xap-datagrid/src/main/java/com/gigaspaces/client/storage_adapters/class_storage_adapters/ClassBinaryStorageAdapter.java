/*
 * Copyright (c) 2008-2019, GigaSpaces Technologies, Inc. All Rights Reserved.
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

package com.gigaspaces.client.storage_adapters.class_storage_adapters;

import com.gigaspaces.api.ExperimentalApi;

import java.io.IOException;
import java.util.Map;

/**
 *  Base class for serialize/deserialize of space EntryData fields Object array into a byte array
 *
 * @author Yael Nahon
 * @since 15.8
 */

@ExperimentalApi
public abstract class ClassBinaryStorageAdapter {

    /***
     * Triggered when object fields need to be retrieved
     * @param serializedFields current serialized fields
     * @return Deserialize fields array
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public abstract Object[] fromBinary(byte[] serializedFields) throws IOException, ClassNotFoundException;

    /***
     * Triggered when object fields need to be be stored
     * @param fields
     * @return Serialized fields array as byte[]
     * @throws IOException
     */
    public abstract byte[] toBinary(Object[] fields) throws IOException;


    /***
     * Triggered when need to access a specific field
     * @param serializedFields current serialized fields
     * @param index
     * @return field value found at requested index in original fields object array
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public abstract Object getFieldAtIndex(byte[] serializedFields, int index) throws IOException, ClassNotFoundException;

    /***
     * Triggered when need to access a several specific fields
     * @param serializedFields current serialized fields
     * @param indexes
     * @return fields values found at requested indexes in original fields object array
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public abstract Object[] getFieldsAtIndexes(byte[] serializedFields, int... indexes) throws IOException, ClassNotFoundException;

    /***
     * Triggered when need to modify a on of the fields stored in a byte[]
     * @param serializedFields current serialized fields
     * @param index
     * @param newValue
     * @return new byte[] of serialized fields after modification
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public abstract byte[] modifyField(byte[] serializedFields, int index, Object newValue) throws IOException, ClassNotFoundException;

    /***
     * Triggered when need to modify a on of the fields stored in a byte[]
     * @param serializedFields current serialized fields
     * @param newValues map from index of field to its new value
     * @return new byte[] of serialized fields after modification
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public abstract byte[] modifyFields(byte[] serializedFields, Map<Integer,Object> newValues) throws IOException, ClassNotFoundException;

    public String getName() {
        return this.getClass().getSimpleName();
    }
}
