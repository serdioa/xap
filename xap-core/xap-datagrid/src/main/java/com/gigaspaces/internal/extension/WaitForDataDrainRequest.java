package com.gigaspaces.internal.extension;

import com.gigaspaces.internal.space.requests.SpaceRequestInfo;
import com.j_spaces.core.SpaceContext;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class WaitForDataDrainRequest implements SpaceRequestInfo {

    static final long serialVersionUID = -216455811647301189L;
    private long timeout;
    private long minTimeToWait;
    private boolean isBackupOnly;
    private SpaceContext context;

    public WaitForDataDrainRequest() {
    }

    public WaitForDataDrainRequest(long timeout, long minTimeToWait, boolean isBackupOnly) {
        this.timeout = timeout;
        this.minTimeToWait = minTimeToWait;
        this.isBackupOnly = isBackupOnly;
    }

    public long getTimeout() {
        return timeout;
    }

    public boolean isBackupOnly() {
        return isBackupOnly;
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
        out.writeBoolean(isBackupOnly);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.timeout = in.readLong();
        this.minTimeToWait = in.readLong();
        this.isBackupOnly = in.readBoolean();
    }
}
