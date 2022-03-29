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
package com.gigaspaces.internal.space.requests;

import com.gigaspaces.dih.consumer.CDCInfo;
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

    public GSMessageRequestInfo() {
    }

    public GSMessageRequestInfo(SpaceDocument document, CDCInfo cdcInfo,
                                GSMessageTask.OperationType operationType, boolean populateDeletedObjectsTable ) {
        this.document = document;
        this.cdcInfo = cdcInfo;
        this.operationType = operationType;
        this.populateDeletedObjectsTable = populateDeletedObjectsTable;
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

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeObject(out, cdcInfo);
        IOUtils.writeObject(out, document);
        IOUtils.writeObject(out, operationType);
        IOUtils.writeObject(out, populateDeletedObjectsTable);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.cdcInfo = IOUtils.readObject(in);
        this.document = IOUtils.readObject(in);
        this.operationType = IOUtils.readObject(in);
        this.populateDeletedObjectsTable = IOUtils.readObject(in);
    }

    @Override
    public boolean enabledSmartExternalizableWithReference() {
        return SpaceRequestInfo.super.enabledSmartExternalizableWithReference();
    }

    @Override
    public String toString() {
        return "Request Info:" + " " +
                "cdcInfo = " + cdcInfo + " " +
                "document = " + document + " " +
                "operationType = " + operationType +
                "populateDeletedObjectsTable = " + populateDeletedObjectsTable ;
    }


}
