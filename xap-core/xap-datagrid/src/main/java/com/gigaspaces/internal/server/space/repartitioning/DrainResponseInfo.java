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


