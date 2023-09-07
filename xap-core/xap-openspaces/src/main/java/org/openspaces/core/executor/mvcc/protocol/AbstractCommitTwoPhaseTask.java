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
package org.openspaces.core.executor.mvcc.protocol;

import com.gigaspaces.internal.client.spaceproxy.IDirectSpaceProxy;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.server.space.mvcc.MVCCGenerationsState;
import net.jini.core.transaction.CannotAbortException;
import net.jini.core.transaction.CannotCommitException;
import net.jini.core.transaction.UnknownTransactionException;
import net.jini.core.transaction.server.TransactionManager;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.executor.TaskGigaSpace;
import org.openspaces.core.executor.mvcc.IMVCCTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;
import java.util.HashSet;

/**
 * @author Sagiv Michael
 * @since 16.3.0
 */
public abstract class AbstractCommitTwoPhaseTask implements IMVCCTask<AbstractMVCCProtocolTaskResult> {
    private static final long serialVersionUID = 8149048216941884516L;
    private static final Logger logger =
            LoggerFactory.getLogger("org.openspaces.core.executor.mvcc.task.commit-two-phase");
    @TaskGigaSpace
    private transient GigaSpace space;
    private final long transactionId;
    private final long committedGeneration;

    protected AbstractCommitTwoPhaseTask(long transactionId, long committedGeneration) {
        this.transactionId = transactionId;
        this.committedGeneration = committedGeneration;
    }

    protected void commitEmbeddedTransaction() throws UnknownTransactionException, RemoteException, CannotCommitException {
        getEmbeddedTransactionManager(getDirectProxy()).commit(transactionId);
        clearGenerationState();
    }

    private void clearGenerationState() {
        space.getSpace().getDirectProxy().setMVCCGenerationsState(null);
    }

    protected void abortEmbeddedTransaction() throws UnknownTransactionException, RemoteException, CannotAbortException {
        getEmbeddedTransactionManager(getDirectProxy()).abort(transactionId);
        clearGenerationState();
    }

    private TransactionManager getEmbeddedTransactionManager(IDirectSpaceProxy directSpaceProxy) {
        final SpaceImpl spaceImpl = directSpaceProxy.getSpaceImplIfEmbedded();
        return spaceImpl.getContainer().getEmbeddedTransactionManager();
    }

    private IDirectSpaceProxy getDirectProxy() {
        IDirectSpaceProxy directSpaceProxy = space.getSpace().getDirectProxy();
        directSpaceProxy.setMVCCGenerationsState(
                new MVCCGenerationsState(committedGeneration, -1L, new HashSet<>()));
        return directSpaceProxy;
    }

    @Override
    public AbstractMVCCProtocolTaskResult execute() throws Exception {
        AbstractMVCCProtocolTaskResult result = executeCommitTask();
        return result;
    }

    @Override
    public long getActiveGeneration() {
        return committedGeneration;
    }

    protected abstract AbstractMVCCProtocolTaskResult executeCommitTask() throws UnknownTransactionException, RemoteException, CannotCommitException, CannotAbortException;
}
