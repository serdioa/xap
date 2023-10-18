package com.gigaspaces.internal.server.space.repartitioning;

import com.gigaspaces.admin.quiesce.QuiesceToken;
import com.gigaspaces.internal.cluster.ClusterTopology;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.space.requests.SpaceRequestInfo;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.j_spaces.core.SpaceContext;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;

public class CopyChunksRequestInfo implements SpaceRequestInfo {
    private static final long serialVersionUID = -2528169887249351498L;
    private SpaceContext context;
    private ClusterTopology newMap;
    private String spaceName;
    private Map<Integer, String> instanceIds;
    private QuiesceToken token;
    private ScaleType scaleType;
    private int generation;

    public CopyChunksRequestInfo() {
    }

    CopyChunksRequestInfo(ClusterTopology newMap, String spaceName, Map<Integer, String> instanceIds, QuiesceToken token, ScaleType scaleType, int generation) {
        this.newMap = newMap;
        this.spaceName = spaceName;
        this.instanceIds = instanceIds;
        this.token = token;
        this.scaleType = scaleType;
        this.generation = generation;
    }

    public ClusterTopology getNewMap() {
        return newMap;
    }

    public String getSpaceName() {
        return spaceName;
    }

    Map<Integer, String> getInstanceIds() {
        return instanceIds;
    }

    public QuiesceToken getToken() {
        return token;
    }

    public int getGeneration() {
        return generation;
    }

    @Override
    public SpaceContext getSpaceContext() {
        return this.context;
    }

    @Override
    public void setSpaceContext(SpaceContext spaceContext) {
        this.context = spaceContext;
    }

    public ScaleType getScaleType() {
        return scaleType;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeObject(out, newMap);
        IOUtils.writeString(out, spaceName);
        IOUtils.writeObject(out, token);
        IOUtils.writeInt(out,generation);
        IOUtils.writeShort(out, (short) instanceIds.size());
        for (Map.Entry<Integer, String> entry : instanceIds.entrySet()) {
            IOUtils.writeShort(out, entry.getKey().shortValue());
            IOUtils.writeString(out, entry.getValue());
        }
        out.writeByte(scaleType.value);
        if (LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v16_4_0)) {
            IOUtils.writeObject(out, context);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.newMap = IOUtils.readObject(in);
        this.spaceName = IOUtils.readString(in);
        this.token = IOUtils.readObject(in);
        this.generation = IOUtils.readInt(in);
        short size = IOUtils.readShort(in);
        this.instanceIds = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            instanceIds.put((int) IOUtils.readShort(in), IOUtils.readString(in));
        }
        this.scaleType = in.readByte() == 0 ? ScaleType.IN : ScaleType.OUT;
        if (LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v16_4_0)) {
            this.context = IOUtils.readObject(in);
        }
    }
}
