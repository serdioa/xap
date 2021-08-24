package com.gigaspaces.internal.server.space.repartitioning;

import com.gigaspaces.admin.quiesce.QuiesceToken;
import com.gigaspaces.internal.client.spaceproxy.executors.SystemTask;
import com.gigaspaces.internal.cluster.ClusterTopology;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.space.requests.SpaceRequestInfo;
import com.gigaspaces.internal.space.responses.SpaceResponseInfo;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;

public class CopyChunksPartitionTask extends SystemTask<SpaceResponseInfo> implements SmartExternalizable {
    private static final long serialVersionUID = -6534857288071741108L;
    private CopyChunksRequestInfo requestInfo;

    public CopyChunksPartitionTask() {
    }

    public CopyChunksPartitionTask(ClusterTopology newMap, String spaceName, Map<Integer, String> instanceIds, QuiesceToken token, ScaleType scaleType) {
        this.requestInfo = new CopyChunksRequestInfo(newMap, spaceName, instanceIds, token, scaleType);
    }

    @Override
    public SpaceRequestInfo getSpaceRequestInfo() {
        return this.requestInfo;
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
