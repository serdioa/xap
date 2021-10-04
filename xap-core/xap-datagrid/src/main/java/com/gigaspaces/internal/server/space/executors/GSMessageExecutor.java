package com.gigaspaces.internal.server.space.executors;

import com.gigaspaces.client.WriteModifiers;
import com.gigaspaces.data_integration.consumer.CDCInfo;
import com.gigaspaces.data_integration.consumer.MessageExecutionException;
import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.space.requests.GSMessageRequestInfo;
import com.gigaspaces.internal.space.requests.SpaceRequestInfo;
import com.gigaspaces.internal.space.responses.SpaceResponseInfo;
import com.j_spaces.core.client.EntryAlreadyInSpaceException;
import net.jini.core.lease.Lease;
import net.jini.core.transaction.*;
import net.jini.core.transaction.server.TransactionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;

public class GSMessageExecutor extends SpaceActionExecutor {

    private final static Logger logger = LoggerFactory.getLogger(GSMessageTask.class);

    @Override
    public SpaceResponseInfo execute(SpaceImpl space, SpaceRequestInfo spaceRequestInfo) {
        GSMessageRequestInfo requestInfo = ((GSMessageRequestInfo) spaceRequestInfo);
        if (!space.getEngine().isTieredStorage()) {
            handleAllInCache(space, requestInfo);
        } else {
            handleTieredStorage(space, requestInfo);
        }
        return null;
    }

    private void handleAllInCache(SpaceImpl space, GSMessageRequestInfo requestInfo) {
        SpaceDocument entry = null;
        CDCInfo cdcInfo = null;
        boolean aborted = false;
        TransactionManager embeddedTransactionManager = space.getContainer().getEmbeddedTransactionManager();
        Transaction transaction = null;
        try {
            transaction = TransactionFactory.create(embeddedTransactionManager, Lease.FOREVER).transaction;
            cdcInfo = requestInfo.getCdcInfo();
            GSMessageTask.Mode mode = requestInfo.getMode();
            entry = requestInfo.getDocument();
            CDCInfo lastMsg = ((CDCInfo) space.getSingleProxy().read(new CDCInfo().setStreamName(cdcInfo.getStreamName()), transaction, 0));
            Long lastMsgID = lastMsg != null ? lastMsg.getMessageID() : 0;

            if (cdcInfo.getMessageID() <= lastMsgID) {
                if (cdcInfo.getMessageID() == 0){
                    logger.warn("processing message with id 0 (full sync)");
                }//todo check with dbsh
                else{
                    logger.warn(String.format("Operation already occurred, ignoring message id: %s, last message id: %s", cdcInfo.getMessageID(), lastMsgID));
                    return;
                }
            }

            space.getSingleProxy().write(cdcInfo, transaction, Lease.FOREVER, 0, WriteModifiers.UPDATE_OR_WRITE.getCode());
            switch (mode) {
                case INSERT:
                    space.getSingleProxy().write(entry, transaction, Lease.FOREVER, 0, WriteModifiers.WRITE_ONLY.getCode());
                    break;
                case UPDATE:
                    space.getSingleProxy().write(entry, transaction, Lease.FOREVER, 0, WriteModifiers.UPDATE_ONLY.getCode());
                    break;
                case DELETE:
                    if (space.getSingleProxy().take(entry, transaction, 0) == null) {
                        logger.error("couldn't delete document: " + entry.getTypeName() + ", message id: " + cdcInfo.getMessageID());
                        throw new MessageExecutionException("couldn't delete document: " + entry.getTypeName() + ", message id: " + cdcInfo.getMessageID());
                    }
                    break;
            }
        } catch (Exception e) {
            logger.warn(String.format("Couldn't complete task execution for Object: %s, message id: %s", entry != null ? entry.getTypeName() : null, cdcInfo != null ? cdcInfo.getMessageID() : null));
            logger.warn("rolling back operation", e);
            if (transaction == null) {
                logger.error("Couldn't rollback, no transaction available");
            } else {
                try {
                    transaction.abort();
                    aborted = true;
                } catch (UnknownTransactionException | CannotAbortException | RemoteException ex) {
                    logger.error("Couldn't complete rollback operation", ex);
                }
            }
            throw new MessageExecutionException(e);
        } finally {
            if (transaction == null) {
                logger.error("Couldn't commit operation, no transaction available");
            } else {
                try {
                    if (!aborted) {
                        transaction.commit();
                    }
                } catch (UnknownTransactionException | CannotCommitException | RemoteException e) {
                    logger.error("Couldn't commit transaction", e);
                }
            }
        }
    }

    private void handleTieredStorage(SpaceImpl space, GSMessageRequestInfo requestInfo) {
        SpaceDocument entry = null;
        try {
            CDCInfo cdcInfo = requestInfo.getCdcInfo();
            GSMessageTask.Mode mode = requestInfo.getMode();
            entry = requestInfo.getDocument();
            CDCInfo lastMsg = ((CDCInfo) space.getSingleProxy().read(new CDCInfo().setStreamName(cdcInfo.getStreamName()), null, 0));
            Long lastMsgID = lastMsg != null ? lastMsg.getMessageID() : 0;
            if (lastMsg != null) {
                if (cdcInfo.getMessageID() < lastMsgID) {
                    logger.warn(String.format("Operation already occurred, ignoring message id: %s, last message id: %s", cdcInfo.getMessageID(), lastMsgID));
                    return;
                }
            }
            space.getSingleProxy().write(cdcInfo, null, Lease.FOREVER, 0, WriteModifiers.UPDATE_OR_WRITE.getCode());
            switch (mode) {
                case INSERT:
                    try {
                        space.getSingleProxy().write(entry, null, Lease.FOREVER, 0, WriteModifiers.WRITE_ONLY.getCode());
                    } catch (EntryAlreadyInSpaceException e) {
                        if (!cdcInfo.getMessageID().equals(lastMsgID)) {
                            logger.error("Couldn't write entry of type: " + entry.getTypeName() + ", for message id: " + cdcInfo.getMessageID());
                            throw e; //might be the first time writing this to space
                        }
                        logger.info("Couldn't write entry of type: " + entry.getTypeName() + ", for message id: " + cdcInfo.getMessageID());
                    }
                    break;
                case UPDATE:
                    space.getSingleProxy().write(entry, null, Lease.FOREVER, 0, WriteModifiers.UPDATE_ONLY.getCode());
                    break;
                case DELETE:
                    if (space.getSingleProxy().take(entry, null, 0) == null) {
                        if (!cdcInfo.getMessageID().equals(lastMsgID)) {
                            logger.error("couldn't delete document: " + entry.getTypeName() + ", message id: " + cdcInfo.getMessageID());
                            throw new MessageExecutionException("couldn't delete document: " + entry.getTypeName() + ", message id: " + cdcInfo.getMessageID());
                        }
                        logger.info("couldn't delete document: " + entry.getTypeName() + ", message id: " + cdcInfo.getMessageID());
                    }
                    break;
            }
        } catch (Exception e) {
            logger.error(String.format("Couldn't complete task execution for Object: %s", entry != null ? entry.getTypeName() : null));
            throw new MessageExecutionException(e);
        }
    }

}
