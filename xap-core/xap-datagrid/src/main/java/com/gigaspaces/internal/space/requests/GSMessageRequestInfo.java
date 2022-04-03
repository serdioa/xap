package com.gigaspaces.internal.space.requests;

import com.gigaspaces.dih.consumer.CDCInfo;
import com.gigaspaces.dih.consumer.configuration.ConflictResolutionPolicy;
import com.gigaspaces.dih.consumer.configuration.GenericType;
import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.server.space.executors.GSMessageTask;
import com.j_spaces.core.SpaceContext;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class GSMessageRequestInfo implements SpaceRequestInfo {

    private static final long serialVersionUID = -920242156459013819L;
    private CDCInfo cdcInfo;
    private SpaceDocument document;
    private GSMessageTask.OperationType operationType;
    private boolean populateDeletedObjectsTable;

    private GenericType genericType;
    private ConflictResolutionPolicy conflictResolutionPolicy;
    private String deletedObjectsTableName;

    public GSMessageRequestInfo() {
    }

    public GSMessageRequestInfo(SpaceDocument document, CDCInfo cdcInfo,
                                GSMessageTask.OperationType operationType, boolean populateDeletedObjectsTable,
                                GenericType genericType, ConflictResolutionPolicy conflictResolutionPolicy,
                                String deletedObjectsTableName ) {
        this.document = document;
        this.cdcInfo = cdcInfo;
        this.operationType = operationType;
        this.populateDeletedObjectsTable = populateDeletedObjectsTable;
        this.genericType = genericType;
        this.conflictResolutionPolicy = conflictResolutionPolicy;
        this.deletedObjectsTableName = deletedObjectsTableName;
    }

    public CDCInfo getCdcInfo() {
        return cdcInfo;
    }

    public SpaceDocument getDocument() {
        return document;
    }

    public GSMessageTask.OperationType getOperationType() {
        return operationType;
    }

    @Override
    public SpaceContext getSpaceContext() {
        return null;
    }

    @Override
    public void setSpaceContext(SpaceContext spaceContext) {

    }

    public boolean isPopulateDeletedObjectsTable() {
        return populateDeletedObjectsTable;
    }

    public GenericType getGenericType() {
        return genericType;
    }

    public ConflictResolutionPolicy getConflictResolutionPolicy() {
        return conflictResolutionPolicy;
    }

    public String getDeletedObjectsTableName() {
        return deletedObjectsTableName;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeObject(out, cdcInfo);
        IOUtils.writeObject(out, document);
        IOUtils.writeObject(out, operationType);
        IOUtils.writeObject(out, populateDeletedObjectsTable);
        IOUtils.writeObject(out, genericType);
        IOUtils.writeObject(out, conflictResolutionPolicy);
        IOUtils.writeObject(out, deletedObjectsTableName);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.cdcInfo = IOUtils.readObject(in);
        this.document = IOUtils.readObject(in);
        this.operationType = IOUtils.readObject(in);
        this.populateDeletedObjectsTable = IOUtils.readObject(in);
        this.genericType = IOUtils.readObject(in);
        this.conflictResolutionPolicy = IOUtils.readObject(in);
        this.deletedObjectsTableName = IOUtils.readObject(in);
    }

    @Override
    public boolean enabledSmartExternalizableWithReference() {
        return SpaceRequestInfo.super.enabledSmartExternalizableWithReference();
    }

    @Override
    public String toString() {
        return "Request Info:" + " " +
                "cdcInfo=" + cdcInfo + " " +
                "document=" + document + " " +
                "operationType=" + operationType +
                "populateDeletedObjectsTable=" + populateDeletedObjectsTable + " " +
                "genericType=" + genericType + " " +
                "conflictResolutionPolicy=" + conflictResolutionPolicy + " " +
                "deletedObjectsTableName=" + deletedObjectsTableName;
    }
}