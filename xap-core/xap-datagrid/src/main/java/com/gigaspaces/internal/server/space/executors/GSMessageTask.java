package com.gigaspaces.internal.server.space.executors;

import com.gigaspaces.internal.client.spaceproxy.executors.SystemTask;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.space.requests.GSMessageRequestInfo;
import com.gigaspaces.internal.space.requests.SpaceRequestInfo;
import com.gigaspaces.internal.space.responses.SpaceResponseInfo;
import com.j_spaces.core.IJSpace;
import net.jini.core.transaction.Transaction;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


public class GSMessageTask extends SystemTask<SpaceResponseInfo> {

    private final static long serialVersionUID = -7309198720798429132L;
    private GSMessageRequestInfo requestInfo;

    public GSMessageTask() {
    }

    public GSMessageTask(GSMessageRequestInfo requestInfo) {
        this.requestInfo = requestInfo;
    }

    public enum Mode {INSERT, UPDATE, DELETE}

    @Override
    public boolean enabledSmartExternalizableWithReference() {
        return super.enabledSmartExternalizableWithReference();
    }

    @Override
    public SpaceRequestInfo getSpaceRequestInfo() {
        return requestInfo;
    }

    @Override
    public SpaceResponseInfo execute(IJSpace spaceProxy, Transaction tx) throws Exception {
        this.requestInfo.setTx(tx);
        SpaceImpl space = spaceProxy.getDirectProxy().getSpaceImplIfEmbedded();
        return space.executeAction(this);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeObject(out, requestInfo);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this.requestInfo = IOUtils.readObject(in);
    }

}
