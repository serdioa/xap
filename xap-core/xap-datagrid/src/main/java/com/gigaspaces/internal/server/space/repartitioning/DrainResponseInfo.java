package com.gigaspaces.internal.server.space.repartitioning;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.space.responses.SpaceResponseInfo;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class DrainResponseInfo implements SpaceResponseInfo{

        static final long serialVersionUID = -835467480688887874L;
        private int partitionId;
        private boolean successful;
        private volatile Exception exception;

        public DrainResponseInfo() {
        }

       DrainResponseInfo(int partitionId) {
            this.partitionId = partitionId;
            this.successful = false;
        }

        public DrainResponseInfo setPartitionId(int partitionId) {
            this.partitionId = partitionId;
            return this;
        }

        public int getPartitionId() {
            return partitionId;
        }

        public boolean isSuccessful() {
            return successful;
        }

        public DrainResponseInfo setSuccessful(boolean successful) {
            this.successful = successful;
            return this;
        }

        public Exception getException() {
            return exception;
        }

        public DrainResponseInfo setException(Exception exception) {
            this.exception = exception;
            return this;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            IOUtils.writeInt(out, partitionId);
            out.writeBoolean(successful);
            IOUtils.writeObject(out, exception);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            this.partitionId = IOUtils.readInt(in);
            this.successful = in.readBoolean();
            this.exception = IOUtils.readObject(in);
        }
    }


