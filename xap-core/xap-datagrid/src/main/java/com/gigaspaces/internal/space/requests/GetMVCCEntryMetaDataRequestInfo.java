package com.gigaspaces.internal.space.requests;

import com.gigaspaces.internal.io.IOUtils;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class GetMVCCEntryMetaDataRequestInfo extends AbstractSpaceRequestInfo{

    private static final long serialVersionUID = -8600456640392621464L;
    private String typeName;
    private Object[] ids;

    public GetMVCCEntryMetaDataRequestInfo(String typeName, Object[] ids) {
        this.typeName = typeName;
        this.ids = ids;
    }

    public GetMVCCEntryMetaDataRequestInfo() {
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public Object[] getIds() {
        return ids;
    }

    public void setId(Object[] ids) {
        this.ids = ids;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeString(out, typeName);
        IOUtils.writeObjectArray(out, ids);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        super.readExternal(in);
        this.typeName = IOUtils.readString(in);
        this.ids = IOUtils.readObjectArray(in);
    }
}
