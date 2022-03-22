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
package com.gigaspaces.internal.server.space.executors;

import com.gigaspaces.client.WriteModifiers;
import com.gigaspaces.dih.consumer.*;
import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.internal.client.spaceproxy.IDirectSpaceProxy;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.server.space.executors.GSMessageTask.OperationType;
import com.gigaspaces.internal.space.requests.GSMessageRequestInfo;
import com.gigaspaces.internal.space.requests.SpaceRequestInfo;
import com.gigaspaces.internal.space.responses.SpaceResponseInfo;
import com.gigaspaces.metadata.SpaceMetadataException;
import com.j_spaces.core.client.EntryAlreadyInSpaceException;
import com.j_spaces.core.client.EntryNotInSpaceException;
import net.jini.core.entry.UnusableEntryException;
import net.jini.core.lease.Lease;
import net.jini.core.transaction.*;
import net.jini.core.transaction.server.TransactionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;

public class GSMessageTaskExecutor extends SpaceActionExecutor {

    private final static Logger logger = LoggerFactory.getLogger(GSMessageTask.class);

    @Override
    public SpaceResponseInfo execute(SpaceImpl space, SpaceRequestInfo spaceRequestInfo) {
        GSMessageRequestInfo requestInfo = ((GSMessageRequestInfo) spaceRequestInfo);
        try {
            if (!space.getEngine().isTieredStorage()) {
                handleAllInCache(space, requestInfo);
            } else {
                handleTieredStorage(space, requestInfo);
            }
        } catch (SkippedMessageExecutionException | NonRetriableMessageExecutionException | RetriableMessageExecutionException e) {
            throw e;
        } catch (EntryAlreadyInSpaceException | EntryNotInSpaceException e) {
            throw new NonRetriableMessageExecutionException(e);
        } catch (Exception e) {
            throw new RetriableMessageExecutionException(e);
        }

        return null;
    }

    private void handleAllInCache(SpaceImpl space, GSMessageRequestInfo requestInfo) throws Exception {
        logger.debug("handling all-in-cache execution: " + requestInfo);
        SpaceDocument entry = null;
        CDCInfo cdcInfo = null;
        Transaction transaction = null;
        try {
            TransactionManager embeddedTransactionManager = space.getContainer().getEmbeddedTransactionManager();
            transaction = TransactionFactory.create(embeddedTransactionManager, Lease.FOREVER).transaction;
            cdcInfo = requestInfo.getCdcInfo();
            OperationType operationType = requestInfo.getOperationType();
            entry = requestInfo.getDocument();
            IDirectSpaceProxy singleProxy = space.getSingleProxy();
            CDCInfo lastMsg = ((CDCInfo) singleProxy.read(new CDCInfo().setPipelineName(cdcInfo.getPipelineName()), null, 0));
            long lastMsgID = lastMsg != null ? lastMsg.getMessageID() : 0;
            if (cdcInfo.getMessageID() <= lastMsgID) {
                if (lastMsgID == 0 && cdcInfo.getMessageID() == 0) {
                    logger.debug("processing message with id 0 (full sync)");
                } else {
                    logger.warn(String.format("operation already occurred, ignoring message id: %s, last message id: %s", cdcInfo.getMessageID(), lastMsgID));
                    return;
                }
            }

            logger.debug("Writing CDCInfo to space: " + cdcInfo);
            singleProxy.write(cdcInfo, transaction, Lease.FOREVER, 0, WriteModifiers.UPDATE_OR_WRITE.getCode());
            switch (operationType) {
                case INSERT:
                    try {
                        logger.debug("inserting message to space: " + entry);
                        singleProxy.write(entry, transaction, Lease.FOREVER, 0, WriteModifiers.WRITE_ONLY.getCode());
                    } catch (EntryAlreadyInSpaceException e) {
                        if (cdcInfo.getMessageID() != 0) {
                            logger.error("failed to write entry of type: " + entry.getTypeName() + ", for message id: " + cdcInfo.getMessageID());
                            throw e; //might be the first time writing this to space
                        }
                        // on a long full sync there might be a failover and therefore retry, so we ignore exceptions like this
                        logger.debug("received same message again, ignoring write entry of type: " + entry.getTypeName() + ", for message id: " + cdcInfo.getMessageID());
                    } catch (SpaceMetadataException e) {
                        throw new SkippedMessageExecutionException(e);
                    }
                    break;
                case UPDATE:
                    logger.debug("update message: " + entry);
                    try {
                        singleProxy.write(entry, transaction, Lease.FOREVER, 0, WriteModifiers.UPDATE_ONLY.getCode());
                    } catch (TransactionException | RemoteException e) {
                        if (isEntryNotInSpaceException(e)) {
                            throw new NonRetriableMessageExecutionException("failed to update entry: " + entry.getTypeName() + ", message id: " + cdcInfo.getMessageID() + " due to EntryNotInSpaceException", e);
                        }
                        throw e;
                    } catch (SpaceMetadataException e) {
                        throw new SkippedMessageExecutionException(e);
                    }
                    break;
                case DELETE:
                    logger.debug("deleting message: " + entry);
                    if (singleProxy.take(entry, transaction, 0) == null) {
                        String errorMsg = "failed to delete entry: " + entry.getTypeName() + ", message id: " + cdcInfo.getMessageID();
                        logger.error(errorMsg);
                        throw new SkippedMessageExecutionException(errorMsg);
                    }
                    break;
            }

            commit(transaction);
        } catch (FailedToCommitException e) {
            logger.warn(String.format("failed to complete task execution for Object: %s, message id: %s", entry != null ? entry.getTypeName() : null, cdcInfo != null ? cdcInfo.getMessageID() : null));
            throw e; //catching an exception from the commit method which means that we would not have to abort
        } catch (Exception e) {
            handleExceptionUnderTransaction(entry, cdcInfo, transaction, e);
            throw e;
        }

    }

    private void handleTieredStorage(SpaceImpl space, GSMessageRequestInfo requestInfo) throws UnusableEntryException, TransactionException, RemoteException, InterruptedException {
        logger.debug("handling tiered storage execution: " + requestInfo);
        SpaceDocument entry = null;
        CDCInfo cdcInfo = null;
        try {
            cdcInfo = requestInfo.getCdcInfo();
            OperationType operationType = requestInfo.getOperationType();
            entry = requestInfo.getDocument();
            IDirectSpaceProxy singleProxy = space.getSingleProxy();
            CDCInfo lastMsg = ((CDCInfo) singleProxy.read(new CDCInfo().setPipelineName(cdcInfo.getPipelineName()), null, 0));
            Long lastMsgID = lastMsg != null ? lastMsg.getMessageID() : 0;
            if (lastMsgID == 0 && cdcInfo.getMessageID() == 0) {
                logger.debug("processing message with id 0 (full sync)");
            } else if (cdcInfo.getMessageID() < lastMsgID) {
                logger.warn(String.format("operation already occurred, ignoring message id: %s, last message id: %s", cdcInfo.getMessageID(), lastMsgID));
                return;
            }

            logger.debug("Writing CDCInfo to space: " + cdcInfo);
            singleProxy.write(cdcInfo, null, Lease.FOREVER, 0, WriteModifiers.UPDATE_OR_WRITE.getCode());
            switch (operationType) {
                case INSERT:
                    try {
                        logger.debug("inserting message to space: " + entry);
                        singleProxy.write(entry, null, Lease.FOREVER, 0, WriteModifiers.WRITE_ONLY.getCode());
                    } catch (EntryAlreadyInSpaceException e) {
                        if (!cdcInfo.getMessageID().equals(lastMsgID)) {
                            logger.error("failed to write entry of type: " + entry.getTypeName() + ", for message id: " + cdcInfo.getMessageID());
                            throw e; //might be the first time writing this to space
                        }
                        logger.debug("received same message again, ignoring write entry of type: " + entry.getTypeName() + ", for message id: " + cdcInfo.getMessageID());
                    } catch (SpaceMetadataException e) {
                        throw new SkippedMessageExecutionException(e);
                    }
                    break;
                case UPDATE:
                    logger.debug("update message: " + entry);
                    try {
                        singleProxy.write(entry, null, Lease.FOREVER, 0, WriteModifiers.UPDATE_ONLY.getCode());
                    } catch (TransactionException | RemoteException e) {
                        if (isEntryNotInSpaceException(e)) {
                            throw new NonRetriableMessageExecutionException("failed to update entry: " + entry.getTypeName() + ", message id: " + cdcInfo.getMessageID() + " due to EntryNotInSpaceException", e);
                        }
                        throw e;
                    } catch (SpaceMetadataException e) {
                        throw new SkippedMessageExecutionException(e);
                    }
                    break;
                case DELETE:
                    logger.debug("deleting message: " + entry);
                    if (singleProxy.take(entry, null, 0) == null) {
                        if (!cdcInfo.getMessageID().equals(lastMsgID)) {
                            String errorMsg = "failed to delete entry: " + entry.getTypeName() + ", message id: " + cdcInfo.getMessageID();
                            logger.error(errorMsg);
                            throw new SkippedMessageExecutionException(errorMsg);
                        }
                        logger.debug("received same message again, ignoring delete entry: " + entry.getTypeName() + ", message id: " + cdcInfo.getMessageID());
                    }
                    break;
            }
        } catch (Exception e) {
            logger.warn(String.format("failed to complete task execution for Object: %s, message id: %s", entry != null ? entry.getTypeName() : null, cdcInfo != null ? cdcInfo.getMessageID() : null));
            throw e;
        }
    }

    private boolean isEntryNotInSpaceException(Throwable e) {
        if (e instanceof EntryNotInSpaceException) {
            return true;
        }
        Throwable cause = e.getCause();
        return cause != null && isEntryNotInSpaceException(cause);
    }

    private void commit(Transaction transaction) {
        if (transaction == null) {
            logger.error("failed to commit operation, no transaction available");
            throw new FailedToCommitException("failed to commit operation, no transaction available");
        }

        try {
            transaction.commit();
        } catch (UnknownTransactionException | CannotCommitException | RemoteException e) {
            logger.error("failed to commit transaction", e);
            throw new FailedToCommitException("failed to commit transaction", e);
        }
    }

    private void handleExceptionUnderTransaction(SpaceDocument entry, CDCInfo cdcInfo, Transaction transaction, Exception e) throws CannotAbortException, UnknownTransactionException, RemoteException {
        logger.warn(String.format("failed to complete task execution for Object: %s, message id: %s", entry != null ? entry.getTypeName() : null, cdcInfo != null ? cdcInfo.getMessageID() : null));
        logger.warn("rolling back operation", e);
        if (transaction == null) {
            logger.error("failed to rollback, no transaction available", e);
            throw new RetriableMessageExecutionException("failed to rollback, no transaction available", e);
        }

        try {
            transaction.abort();
        } catch (UnknownTransactionException | CannotAbortException | RemoteException ex) {
            logger.error("failed to rollback", ex);
            throw new RetriableMessageExecutionException("failed to rollback", ex);
        }
    }

}
