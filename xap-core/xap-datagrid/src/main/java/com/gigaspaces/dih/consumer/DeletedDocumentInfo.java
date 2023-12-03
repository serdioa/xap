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
package com.gigaspaces.dih.consumer;

import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.metadata.SpaceTypeDescriptorBuilder;

public class DeletedDocumentInfo extends SpaceDocument {

    private static final long serialVersionUID = 1L;

    public static final String PROPERTY_ID = "ID";
    public static final String PROPERTY_FULL_TYPE_NAME = "TypeName";
    public static final String PROPERTY_TIMESTAMP = "Timestamp";

    public static SpaceTypeDescriptor getTypeDescriptor( String typeName ) {
        return new SpaceTypeDescriptorBuilder( typeName )
                .idProperty(PROPERTY_ID, false)
                .addFixedProperty(PROPERTY_ID, String.class)
                .addFixedProperty(PROPERTY_FULL_TYPE_NAME, String.class)
                .addFixedProperty(PROPERTY_TIMESTAMP, Long.class)
                .supportsDynamicProperties(false)
                .create();
    }

    public DeletedDocumentInfo(String deletedObjectsTableName, String typeName, String id) {
        super(deletedObjectsTableName);
        setFullTypeName(typeName);
        setId(id);
        setTimestamp(System.currentTimeMillis() );
    }

    public DeletedDocumentInfo setId(String id) {
        setProperty(PROPERTY_ID, id);
        return this;
    }

    public DeletedDocumentInfo setTimestamp(long timestamp) {
        setProperty(PROPERTY_TIMESTAMP, timestamp);
        return this;
    }

    public DeletedDocumentInfo setFullTypeName(String typeName) {
        setProperty(PROPERTY_FULL_TYPE_NAME, typeName);
        return this;
    }

    public String getId() {
        return getProperty(PROPERTY_ID);
    }

    public Long getTimestamp() {
        return getProperty(PROPERTY_TIMESTAMP);
    }

    public String getFullTypeName() {
        return getProperty(PROPERTY_FULL_TYPE_NAME);
    }

    @Override
    public String toString() {
        return "id = " + getId() + " " +
                "timestamp = " + getTimestamp() + " " +
                "typeName = " + getTypeName() + " " +
                "fullTypeName =" + getFullTypeName();
    }
}