package com.gigaspaces.internal.space.requests;

import com.gigaspaces.internal.io.IOUtils;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class GetMVCCEntryMetaDataRequestInfo extends AbstractSpaceRequestInfo{

    private static final long serialVersionUID = -7255029922496067038L;
    private String typeName;
    private Object[] ids;
    private long transactionId;

    public GetMVCCEntryMetaDataRequestInfo(String typeName, Object[] ids, long transactionId) {
        this.typeName = typeName;
        this.ids = ids;
        this.transactionId = transactionId;
    }

    public GetMVCCEntryMetaDataRequestInfo() {
    }

    public String getTypeName() {
        return typeName;
    }

    public Object[] getIds() {
        return ids;
    }


    public long getTransactionId() {
        return transactionId;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeString(out, typeName);
        IOUtils.writeObjectArray(out, ids);
        IOUtils.writeLong(out, transactionId);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        super.readExternal(in);
        this.typeName = IOUtils.readString(in);
        this.ids = IOUtils.readObjectArray(in);
        this.transactionId = IOUtils.readLong(in);
    }
}
