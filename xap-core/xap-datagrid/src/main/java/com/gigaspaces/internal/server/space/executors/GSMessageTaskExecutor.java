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
import com.gigaspaces.dih.consumer.configuration.ConflictResolutionPolicy;
import com.gigaspaces.dih.consumer.configuration.GenericType;
import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.entry.CompoundSpaceId;
import com.gigaspaces.internal.client.QueryResultTypeInternal;
import com.gigaspaces.internal.client.spaceproxy.IDirectSpaceProxy;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.server.space.executors.GSMessageTask.OperationType;
import com.gigaspaces.internal.space.requests.GSMessageRequestInfo;
import com.gigaspaces.internal.space.requests.SpaceRequestInfo;
import com.gigaspaces.internal.space.responses.SpaceResponseInfo;
import com.gigaspaces.metadata.SpaceMetadataException;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.j_spaces.core.client.EntryAlreadyInSpaceException;
import com.j_spaces.core.client.EntryNotInSpaceException;
import com.j_spaces.core.client.Modifiers;
import com.j_spaces.kernel.JSpaceUtilities;
import net.jini.core.entry.UnusableEntryException;
import net.jini.core.lease.Lease;
import net.jini.core.transaction.*;
import net.jini.core.transaction.server.TransactionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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

            GenericType genericType = requestInfo.getGenericType();
            ConflictResolutionPolicy conflictResolutionPolicy = requestInfo.getConflictResolutionPolicy();
            String deletedObjectsTableName = requestInfo.getDeletedObjectsTableName();
            if( logger.isDebugEnabled() ) {
                logger.debug("--- operation type:" + operationType + ", allInCache, genericType=" + genericType + ", conflictResolutionPolicy=" +
                        conflictResolutionPolicy + ", isPopulateDeletedObjectsTable=" + requestInfo.isPopulateDeletedObjectsTable() +
                        ", deletedObjectsTableName=" + deletedObjectsTableName);
            }

            switch (operationType) {
                case INSERT:
                    boolean isWriteAllowed =
                            isWriteEntryAllowed( singleProxy, deletedObjectsTableName, entry, transaction, genericType, conflictResolutionPolicy );
                    if ( isWriteAllowed ) {
                        try {
                            logger.debug("inserting message to space: " + entry);
                            singleProxy.write(entry, transaction, Lease.FOREVER, 0, WriteModifiers.WRITE_ONLY.getCode());
                        } catch (EntryAlreadyInSpaceException e) {
                            if (cdcInfo.getMessageID() != 0) {
                                logger.error("failed to write entry of type: " + entry.getTypeName() + ", for message id: " + cdcInfo.getMessageID());
                                throw e; //might be the first time writing this to space
                            }
                            // on a long full sync there might be a failover and therefore retry, so we ignore exceptions like this
                            logger.debug("received same message again, ignoring write entry of type: " +
                                    entry.getTypeName() + ", for message id: " + cdcInfo.getMessageID());
                        } catch (SpaceMetadataException e) {
                            throw new SkippedMessageExecutionException(e);
                        }
                    }
                    break;
                case UPDATE:
                    isWriteAllowed =
                            isWriteEntryAllowed( singleProxy, deletedObjectsTableName, entry, transaction, genericType, conflictResolutionPolicy );
                    if( isWriteAllowed ) {
                        logger.debug("update message: " + entry);
                        try {
                            singleProxy.write(entry, transaction, Lease.FOREVER, 0, WriteModifiers.UPDATE_ONLY.getCode());
                        } catch (TransactionException | RemoteException e) {
                            if (isEntryNotInSpaceException(e)) {
                                throw new NonRetriableMessageExecutionException("failed to update entry: " +
                                        entry.getTypeName() + ", message id: " + cdcInfo.getMessageID() + " due to EntryNotInSpaceException", e);
                            }
                            throw e;
                        } catch (SpaceMetadataException e) {
                            throw new SkippedMessageExecutionException(e);
                        }
                    }
                    break;
                case DELETE:
                    logger.debug("deleting message: " + entry);
                    if( requestInfo.isPopulateDeletedObjectsTable() && genericType == GenericType.FROM_CDC ){

                        SpaceTypeDescriptor typeDescriptor = null;
                        try{
                            typeDescriptor = singleProxy.getTypeDescriptor(entry.getTypeName());
                        } catch (RemoteException e) {
                            logger.error( "Failed to retrieve type descriptor for type [" + entry.getTypeName() + "] due to ", e );
                        }
                        if (typeDescriptor == null) {
                            throw new NonRetriableMessageExecutionException("Unknown type: " + entry.getTypeName());
                        }

                        DeletedDocumentInfo deletedSpaceDocument =
                                        createDeletedSpaceDocument( deletedObjectsTableName, typeDescriptor, entry );
                        logger.debug("writing deleted message(all in cache): " + deletedSpaceDocument );
                        try {
                            singleProxy.write( deletedSpaceDocument, null, Lease.FOREVER );
                        }
                        catch( Exception e ){
                            logger.error("failed to write entry of type: " + deletedSpaceDocument.getFullTypeName() +
                                    " to deleted table: " + deletedSpaceDocument.getTypeName() +
                                    " with id: " + deletedSpaceDocument.getId() + " due to ", e );
                        }
                    }
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

            GenericType genericType = requestInfo.getGenericType();
            ConflictResolutionPolicy conflictResolutionPolicy = requestInfo.getConflictResolutionPolicy();
            String deletedObjectsTableName = requestInfo.getDeletedObjectsTableName();
            if (logger.isDebugEnabled()){
                logger.debug("--- operation type:" + operationType + ", TieredStorage, genericType=" + genericType + ", conflictResolutionPolicy=" +
                        conflictResolutionPolicy + ", isPopulateDeletedObjectsTable=" + requestInfo.isPopulateDeletedObjectsTable() );
            }

            switch (operationType) {
                case INSERT:
                    boolean isWriteAllowed =
                            isWriteEntryAllowed( singleProxy, deletedObjectsTableName, entry, null, genericType, conflictResolutionPolicy );
                    if ( isWriteAllowed ) {
                        try {
                            logger.debug("inserting message to space: " + entry);
                            singleProxy.write(entry, null, Lease.FOREVER, 0, WriteModifiers.WRITE_ONLY.getCode());
                        } catch (EntryAlreadyInSpaceException e) {
                            if (!cdcInfo.getMessageID().equals(lastMsgID)) {
                                logger.error("failed to write entry of type: " + entry.getTypeName() + ", for message id: " + cdcInfo.getMessageID());
                                throw e; //might be the first time writing this to space
                            }
                            logger.debug("received same message again, ignoring write entry of type: " +
                                    entry.getTypeName() + ", for message id: " + cdcInfo.getMessageID());
                        } catch (SpaceMetadataException e) {
                            throw new SkippedMessageExecutionException(e);
                        }
                    }
                    break;
                case UPDATE:
                    //CDC case
                    isWriteAllowed =
                            isWriteEntryAllowed( singleProxy, deletedObjectsTableName, entry, null, genericType, conflictResolutionPolicy );
                    if( isWriteAllowed ) {
                        logger.debug("update message: " + entry);
                        try {
                            singleProxy.write(entry, null, Lease.FOREVER, 0, WriteModifiers.UPDATE_ONLY.getCode());
                        } catch (TransactionException | RemoteException e) {
                            if (isEntryNotInSpaceException(e)) {
                                throw new NonRetriableMessageExecutionException("failed to update entry: " +
                                        entry.getTypeName() + ", message id: " + cdcInfo.getMessageID() + " due to EntryNotInSpaceException", e);
                            }
                            throw e;
                        } catch (SpaceMetadataException e) {
                            throw new SkippedMessageExecutionException(e);
                        }
                    }
                    break;
                case DELETE:
                    logger.debug("deleting message: " + entry);
                    if( requestInfo.isPopulateDeletedObjectsTable() && genericType == GenericType.FROM_CDC ){

                        SpaceTypeDescriptor typeDescriptor = null;
                        try{
                            typeDescriptor = singleProxy.getTypeDescriptor(entry.getTypeName());
                        } catch (RemoteException e) {
                            logger.error( "Failed to retrieve type descriptor for type [" + entry.getTypeName() + "] due to ", e );
                        }
                        if (typeDescriptor == null) {
                            throw new NonRetriableMessageExecutionException("Unknown type: " + entry.getTypeName());
                        }

                        DeletedDocumentInfo deletedSpaceDocument =
                                createDeletedSpaceDocument( deletedObjectsTableName, typeDescriptor, entry);
                        logger.debug("writing deleted message(tieredStorage): " + deletedSpaceDocument);
                        try{
                            singleProxy.write( deletedSpaceDocument, null, Lease.FOREVER );
                        }
                        catch( Exception e ){
                            logger.error("failed to write entry of type: " + deletedSpaceDocument.getFullTypeName() +
                                    " to deleted table: " + deletedSpaceDocument.getTypeName() +
                                    " with id: " + deletedSpaceDocument.getId() + " due to ", e );
                        }
                    }
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

    private DeletedDocumentInfo createDeletedSpaceDocument( String deletedObjectTableName,
                                                            SpaceTypeDescriptor typeDescriptor, SpaceDocument spaceDocument ) {

        List<String> idPropertiesNames = typeDescriptor.getIdPropertiesNames();
        Object idValue = getIdValues(idPropertiesNames, spaceDocument);
        if( logger.isDebugEnabled() ) {
            logger.debug("Deleted object id:" + idValue);
        }
        return new DeletedDocumentInfo( deletedObjectTableName, spaceDocument.getTypeName(), idValue.toString() );
    }

    private boolean isWriteEntryAllowed( IDirectSpaceProxy singleProxy, String deletedObjectsTableName, SpaceDocument entry,
                                         Transaction transaction, GenericType genericType,
                                         ConflictResolutionPolicy conflictResolutionPolicy ) {
        boolean writeAllowed = true;
        //TODO add warn about update + GenericType.FROM_INITIAL_LOAD ans skip such message

        if( logger.isDebugEnabled() ) {
            logger.debug("isWriteEntryAllowed, genericType=" + genericType +
                    ", conflictResolutionPolicy=" + conflictResolutionPolicy +
                    ", deletedObjectsTableName=" + deletedObjectsTableName);
        }

        if( genericType == GenericType.FROM_INITIAL_LOAD && conflictResolutionPolicy == ConflictResolutionPolicy.INITIAL_LOAD ){
            boolean entryInDeletedTableOfSpace = isEntryInDeletedTableOfSpace(singleProxy, deletedObjectsTableName, entry, transaction);
            writeAllowed = !entryInDeletedTableOfSpace;
        }

        return writeAllowed;
    }

    private boolean isEntryInDeletedTableOfSpace( IDirectSpaceProxy singleProxy, String deletedObjectsTableName,
                                                  SpaceDocument entry, Transaction transaction ){

        if( deletedObjectsTableName == null ){
            throw new IllegalArgumentException("Name of deletedObjectsTableName can't be null");
        }

        SpaceTypeDescriptor typeDescriptor = null;
        try {
            typeDescriptor = singleProxy.getTypeDescriptor(entry.getTypeName());
        } catch (RemoteException e) {
            logger.error( "Failed to retrieve type descriptor for type [" + entry.getTypeName() + "] due to ", e );
        }
        if (typeDescriptor == null) {
            throw new NonRetriableMessageExecutionException("Unknown type: " + entry.getTypeName());
        }
        List<String> idPropertiesNames = typeDescriptor.getIdPropertiesNames();
        Object idValues = getIdValues( idPropertiesNames, entry).toString();
        String routingPropertyName = typeDescriptor.getRoutingPropertyName();
        Object routingPropertyValue = idPropertiesNames.contains( routingPropertyName ) ?
                                                    null : entry.getProperty(routingPropertyName);

        if( logger.isDebugEnabled() ) {
            logger.debug("isEntryInDeletedTableOfSpace, deletedObjectsTableName=" +
                    deletedObjectsTableName + ", id=" + idValues);
        }

        Object[] readObjects = null;
        try {
            readObjects = singleProxy.readByIds(deletedObjectsTableName, new Object[]{idValues},
                    routingPropertyValue, transaction, Modifiers.NONE,
                    QueryResultTypeInternal.DOCUMENT_ENTRY, false, null);

            if (logger.isDebugEnabled()){
                logger.debug("isEntryInDeletedTableOfSpace, result=" + readObjects +
                        (readObjects != null ? Arrays.toString(readObjects) : "NULL result array"));
            }
        }
        catch( Exception e ){
            if( logger.isErrorEnabled() ){
                logger.error( "Failed to read objects from space using id [" + idValues + "] from ", e );
            }
        }

        boolean isEntryInDeletedTableOfSpace = readObjects != null && readObjects.length > 0 &&
                                                !JSpaceUtilities.areAllArrayElementsNull( readObjects );
        if (logger.isDebugEnabled()) {
            logger.debug("isEntryInDeletedTableOfSpace, isEntryInDeletedTableOfSpace=" + isEntryInDeletedTableOfSpace);
        }

        return isEntryInDeletedTableOfSpace;
    }

    private Object getIdValues( List<String> idPropertiesNames, SpaceDocument entry) {

        List<Object> values = new ArrayList<>();
        values.add( entry.getTypeName() );
        for (String idPropertyName : idPropertiesNames) {
            values.add( entry.getProperty(idPropertyName) );
        }

        return CompoundSpaceId.from( values.toArray( new Object[0] ) );
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

    private void handleExceptionUnderTransaction(SpaceDocument entry, CDCInfo cdcInfo, Transaction transaction, Exception e) {
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