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

package com.gigaspaces.internal.cluster.node;

import com.gigaspaces.internal.cluster.node.impl.backlog.sync.IMarker;
import com.gigaspaces.internal.cluster.node.impl.directPersistency.IDirectPersistencyOpInfo;
import com.gigaspaces.internal.cluster.node.impl.directPersistency.IDirectPersistencySyncHandler;
import com.gigaspaces.internal.server.space.mvcc.MVCCGenerationsState;

public interface IReplicationOutContext {
    int pendingSize();

    void release();

    void askForMarker(String membersGroupName);

    IMarker getRequestedMarker();

    int getCompleted();

    int setCompleted(int completed);

    void setDirectPersistencyPendingEntry(IDirectPersistencyOpInfo directPesistencyEntry);

    IDirectPersistencyOpInfo getDirectPersistencyPendingEntry();

    IDirectPersistencySyncHandler getDirectPesistencySyncHandler();

    void setDirectPesistencySyncHandler(IDirectPersistencySyncHandler directPesistencySyncHandler);

    int getBlobstoreReplicationBulkId();

    void setBlobstoreReplicationBulkId(int blobstoreBulkId);

    boolean isBlobstorePendingReplicationBulk();

    void blobstorePendingReplicationBulk();

    MVCCGenerationsState getMVCCGenerationsState();

    void setMVCCGenerationsState(MVCCGenerationsState mvccGenerationsState);
}
