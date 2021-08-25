package com.gigaspaces.internal.space.requests;

import com.gigaspaces.data_integration.consumer.CDCInfo;
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


    public GSMessageRequestInfo() {
    }

    public GSMessageRequestInfo(SpaceDocument document, CDCInfo cdcInfo, GSMessageTask.OperationType operationType) {
        this.document = document;
        this.cdcInfo = cdcInfo;
        this.operationType = operationType;
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

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeObject(out, cdcInfo);
        IOUtils.writeObject(out, document);
        IOUtils.writeObject(out, operationType);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.cdcInfo = IOUtils.readObject(in);
        this.document = IOUtils.readObject(in);
        this.operationType = IOUtils.readObject(in);
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
                "operationType = " + operationType;
    }
}
