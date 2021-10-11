package com.gigaspaces.internal.server.space.repartitioning;

import com.gigaspaces.internal.space.requests.SpaceRequestInfo;
import com.j_spaces.core.SpaceContext;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class DrainRequestInfo implements SpaceRequestInfo {
    static final long serialVersionUID = -216455811647301189L;
    private long timeout;
    private long minTimeToWait;
    private boolean isComprehensive;
    private SpaceContext context;


    public DrainRequestInfo() {
    }

    public DrainRequestInfo(long timeout, long minTimeToWait, boolean isComprehensive) {
        this.timeout = timeout;
        this.minTimeToWait = minTimeToWait;
        this.isComprehensive = isComprehensive;
    }

    public long getTimeout() {
        return timeout;
    }

    public boolean isComprehensive() {
        return isComprehensive;
    }

    @Override
    public SpaceContext getSpaceContext() {
        return context;
    }

    @Override
    public void setSpaceContext(SpaceContext spaceContext) {
        this.context = spaceContext;
    }

    public long getMinTimeToWait() {
        return minTimeToWait;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(timeout);
        out.writeLong(minTimeToWait);
        out.writeBoolean(isComprehensive);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.timeout = in.readLong();
        this.minTimeToWait = in.readLong();
        this.isComprehensive = in.readBoolean();
    }
}
