package com.gigaspaces.internal.server.space.executors;

import com.gigaspaces.dih.consumer.NonRetriableMessageExecutionException;
import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.entry.CompoundSpaceId;
import com.gigaspaces.internal.client.QueryResultTypeInternal;
import com.gigaspaces.internal.client.spaceproxy.IDirectSpaceProxy;
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.j_spaces.core.client.Modifiers;
import com.j_spaces.kernel.JSpaceUtilities;
import net.jini.core.transaction.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class GSMessageTaskExecutorUtils {

    private final static Logger logger = LoggerFactory.getLogger(GSMessageTaskExecutorUtils.class);

    static boolean isEntryInDeletedTableOfSpace(IDirectSpaceProxy singleProxy, String deletedObjectsTableName,
                                                        SpaceDocument entry, Transaction transaction ){

        Object[] readObjects = getEntriesFromDeletedObjectsTable( singleProxy, deletedObjectsTableName, entry, transaction );

        boolean isEntryInDeletedTableOfSpace = readObjects != null && readObjects.length > 0 &&
                !JSpaceUtilities.areAllArrayElementsNull( readObjects );
        if (logger.isDebugEnabled()) {
            logger.debug("isEntryInDeletedTableOfSpace, isEntryInDeletedTableOfSpace=" + isEntryInDeletedTableOfSpace);
        }

        return isEntryInDeletedTableOfSpace;
    }

    public static String getIdValue( SpaceTypeDescriptor typeDescriptor, SpaceDocument spaceDocument) {

        List<Object> values = new ArrayList<>();
        values.add(typeDescriptor.getTypeName());
        for (String idPropertyName : typeDescriptor.getIdPropertiesNames()) {
            values.add(spaceDocument.getProperty(idPropertyName));
        }

        return CompoundSpaceId.from(values.toArray(new Object[0])).toString();
    }

    private static Object[] getEntriesFromDeletedObjectsTable(IDirectSpaceProxy singleProxy, String deletedObjectsTableName,
                                                             SpaceDocument spaceDocument, Transaction transaction ){

        String idValue = getIdValue(getTypeDescriptor(singleProxy, spaceDocument.getTypeName()), spaceDocument);

        if( logger.isDebugEnabled() ) {
            logger.debug("isEntryInDeletedTableOfSpace, deletedObjectsTableName=" +
                    deletedObjectsTableName + ", id=" + idValue);
        }

        Map<String,SpaceDocument> singleSpaceDocumentMap = new HashMap<>(1);
        singleSpaceDocumentMap.put( idValue, spaceDocument );
        return getEntriesFromDeletedObjectsTable( singleProxy, deletedObjectsTableName, singleSpaceDocumentMap, transaction );
    }

    public static Object[] getEntriesFromDeletedObjectsTable(IDirectSpaceProxy singleProxy, String deletedObjectsTableName,
                                                             Map<String, SpaceDocument> spaceDocuments, Transaction transaction ){

        if( deletedObjectsTableName == null ){
            throw new IllegalArgumentException("Name of deletedObjectsTableName can't be null");
        }

        if( spaceDocuments.isEmpty() ){
            return new Object[0];
        }

        if( singleProxy.getTypeDescFromServer( deletedObjectsTableName ) == null ){
            return new Object[0];
        }

        String[] idValues = spaceDocuments.keySet().toArray(new String[0]);
        Object[] readObjects = null;
        try {
            ISpaceProxy clusteredProxy = (ISpaceProxy)singleProxy.getClusteredProxy();
            readObjects = clusteredProxy.readByIds(deletedObjectsTableName, idValues,
                    null, transaction, Modifiers.NONE, QueryResultTypeInternal.DOCUMENT_ENTRY, false, null);

            if (logger.isDebugEnabled()){
                logger.debug("getEntriesFromDeletedObjectsTable, deletedObjectsTableName=" +
                        deletedObjectsTableName + ", id=" + Arrays.toString( idValues ) + ", result=" + readObjects +
                        (readObjects != null ? Arrays.toString(readObjects) : "NULL result array"));
            }
        }
        catch( Exception e ){
            if( logger.isErrorEnabled() ){
                logger.error( "Failed to read objects from space using id [" + Arrays.toString( idValues ) + "] from ", e );
            }
        }

        return readObjects;
    }

    private static SpaceTypeDescriptor getTypeDescriptor(IDirectSpaceProxy singleProxy, String typeName) {
        SpaceTypeDescriptor typeDescriptor;
        try{
            typeDescriptor = singleProxy.getTypeDescriptor(typeName);
        } catch (Exception e) {
            logger.error( "Failed to retrieve type descriptor for type [" + typeName + "] due to ", e );
            throw new NonRetriableMessageExecutionException("Unknown type: " + typeName);
        }

        return typeDescriptor;
    }
}