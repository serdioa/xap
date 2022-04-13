package com.gigaspaces.dih.consumer;

import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.metadata.SpaceTypeDescriptorBuilder;

public class DeletedDocumentInfo extends SpaceDocument {

    private static final long serialVersionUID = 1L;

    private static final String PROPERTY_ID = "ID";
    private static final String PROPERTY_FULL_TYPE_NAME = "TypeName";
    private static final String PROPERTY_TIMESTAMP = "Timestamp";

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