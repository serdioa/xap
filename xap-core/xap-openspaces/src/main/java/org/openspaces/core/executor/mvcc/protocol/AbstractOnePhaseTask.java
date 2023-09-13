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
import net.jini.core.lease.LeaseDeniedException;
import net.jini.core.transaction.Transaction;
import net.jini.core.transaction.TransactionFactory;
import net.jini.core.transaction.server.TransactionManager;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.executor.TaskGigaSpace;
import org.openspaces.core.executor.mvcc.IMVCCTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;
import java.time.Duration;

/**
 * @author Sagiv Michael
 * @since 16.3.0
 */
public abstract class AbstractOnePhaseTask implements IMVCCTask<AbstractMVCCProtocolTaskResult> {
    private static final long serialVersionUID = 5714510450727020854L;
    private static final Logger logger = LoggerFactory.getLogger("org.openspaces.core.executor.mvcc.task.one-phase");
    public static final long DEFAULT_TRANSACTION_LEASE = Duration.ofMinutes(5).toMillis();
    @TaskGigaSpace
    private transient GigaSpace space;
    private IDirectSpaceProxy directSpaceProxy;
    private final MVCCGenerationsState generationsState;
    private int taskRetries;
    private long retryIntervalMillis;

    protected AbstractOnePhaseTask(MVCCGenerationsState generationsState, int taskRetries, long retryIntervalMillis) {
        this.generationsState = generationsState;
        this.taskRetries = taskRetries;
        this.retryIntervalMillis = retryIntervalMillis;
    }

    private Transaction createEmbeddedTransaction() throws LeaseDeniedException, RemoteException {
        final SpaceImpl spaceImpl = directSpaceProxy.getSpaceImplIfEmbedded();
        final TransactionManager embeddedTransactionManager = spaceImpl.getContainer().getEmbeddedTransactionManager();
        return TransactionFactory.create(embeddedTransactionManager, DEFAULT_TRANSACTION_LEASE).transaction;
    }

    private void setDirectProxyWithMvccGeneration() {
        directSpaceProxy = space.getSpace().getDirectProxy();
        directSpaceProxy.setMVCCGenerationsState(generationsState);
    }

    @Override
    public AbstractMVCCProtocolTaskResult execute() throws Exception {
        final long startTime = System.currentTimeMillis();
        AbstractMVCCProtocolTaskResult result = runTask();
        result.setExecutionTime(System.currentTimeMillis() - startTime);
        return result;
    }

    private AbstractMVCCProtocolTaskResult runTask() throws Exception {
        setDirectProxyWithMvccGeneration();
        final Transaction transaction = createEmbeddedTransaction();
        try {
            AbstractMVCCProtocolTaskResult result = executeTask(directSpaceProxy, transaction);
            result.addActiveGeneration(getActiveGeneration());
            transaction.commit();
            clearGenerationState();
            return result;
        } catch (Exception e) {
            logger.warn("Exception caught while executing task", e);
            logger.info("Aborting transaction {} with generation {}.",  transaction, getActiveGeneration());
            transaction.abort();
            if (taskRetries-- > 0) {
                logger.info("Failed to complete task, retrying with interval of {} milliseconds.", retryIntervalMillis);
                Thread.sleep(retryIntervalMillis);
                retryIntervalMillis = retryIntervalMillis * 2;
                return runTask();
            }
            return createFailedTaskResult(e);
        }
    }

    @Override
    public long getActiveGeneration() {
        return generationsState.getNextGeneration();
    }

    private void clearGenerationState() {
        space.getSpace().getDirectProxy().setMVCCGenerationsState(null);
    }

    protected abstract AbstractMVCCProtocolTaskResult executeTask(IDirectSpaceProxy proxy, Transaction transaction)
            throws Exception;

    protected abstract AbstractMVCCProtocolTaskResult createFailedTaskResult(Throwable e);

    public IDirectSpaceProxy getDirectSpaceProxy() {
        return directSpaceProxy;
    }
}
