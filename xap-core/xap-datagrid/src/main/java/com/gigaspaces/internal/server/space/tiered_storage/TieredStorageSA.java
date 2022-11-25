package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.metadata.TypeCounters;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.gigaspaces.internal.server.space.tiered_storage.transaction.*;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.metadata.index.SpaceIndex;
import com.gigaspaces.sync.SpaceSynchronizationEndpoint;
import com.j_spaces.core.SpaceOperations;
import com.j_spaces.core.cache.InitialLoadInfo;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.cache.context.TieredState;
import com.j_spaces.core.sadapter.ISAdapterIterator;
import com.j_spaces.core.sadapter.IStorageAdapter;
import com.j_spaces.core.sadapter.SAException;
import net.jini.core.transaction.server.ServerTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

public class TieredStorageSA implements IStorageAdapter {
    private final Logger logger;
    private final InternalRDBMS internalRDBMS;
    private final SpaceEngine engine;
    private IStorageAdapter externalInitialLoadSA;
    private boolean loadedFromMirror = false;

    public TieredStorageSA(InternalRDBMS internalRDBMS, SpaceEngine engine) {
        this.internalRDBMS = internalRDBMS;
        this.engine = engine;
        this.logger = LoggerFactory.getLogger(this.getClass().getName() + engine.getFullSpaceName());
    }

    public void setExternalInitialLoadSA(IStorageAdapter externalInitialLoadSA) {
        this.externalInitialLoadSA = externalInitialLoadSA;
    }

    public boolean initializeInternalRDBMS(String spaceName, String fullMemberName, SpaceTypeManager typeManager, boolean isBackup) throws SAException {
        if (logger.isDebugEnabled()) {
            logger.debug("call initializeInternalRDBMS");
        }
        return internalRDBMS.initialize(spaceName, fullMemberName, typeManager, isBackup);
    }

    public long getDiskSize() throws SAException, IOException {
        if (logger.isDebugEnabled()) {
            logger.debug("call getDiskSize");
        }
        return internalRDBMS.getDiskSize();
    }

    public long getFreeSpaceSize() throws SAException, IOException {
        if (logger.isDebugEnabled()) {
            logger.debug("call getFreeSpaceSize");
        }
        return internalRDBMS.getFreeSpaceSize();
    }

    public void createTable(ITypeDesc typeDesc) throws SAException {
        if (logger.isDebugEnabled()) {
            logger.debug("call createTable");
        }
        internalRDBMS.createTable(typeDesc);
    }

    public void dropTable(ITypeDesc typeDesc) throws SAException {
        if (logger.isDebugEnabled()) {
            logger.debug("call dropTable");
        }
        internalRDBMS.dropTable(typeDesc);
    }

    public ISAdapterIterator<IEntryHolder> makeSingleTypeEntriesIter(String typeName, ITemplateHolder templateHolder) throws SAException {
        if (logger.isDebugEnabled()) {
            logger.debug("call makeSingleTypeEntriesIter");
        }
        ISAdapterIterator<IEntryHolder> iEntryHolderISAdapterIterator = internalRDBMS.makeEntriesIter(typeName, templateHolder);

        if (templateHolder != null && templateHolder.isReadOperation()) {
            templateHolder.getServerTypeDesc().getTypeCounters().incDiskReadCounter();
        }

        return iEntryHolderISAdapterIterator;
    }

    public boolean isKnownType(String name) {
        if (logger.isDebugEnabled()) {
            logger.debug("call isKnownType");
        }
        return internalRDBMS.isKnownType(name);
    }

    @Override
    public void initialize() throws SAException {
        if (logger.isDebugEnabled()) {
            logger.debug("call initialize");
        }
        if (externalInitialLoadSA != null)
            externalInitialLoadSA.initialize();
    }

    @Override
    public ISAdapterIterator initialLoad(Context context, ITemplateHolder template, InitialLoadInfo initialLoadInfo) throws SAException {
        if (logger.isDebugEnabled()) {
            logger.debug("call initialLoad");
        }
        if (template == null && initialLoadInfo != null) {
            internalRDBMS.initialLoad(context, engine, initialLoadInfo);
        } else if (!loadedFromMirror && externalInitialLoadSA != null) {
            loadedFromMirror = true;
            return externalInitialLoadSA.initialLoad(context, template, null);
        }
        return null;
    }

    //origin = not from replication
    @Override
    public void insertEntry(Context context, IEntryHolder entryHolder, boolean origin, boolean shouldReplicate) throws SAException {
        if (logger.isDebugEnabled()) {
            logger.debug("call insertEntry");
        }
        TypeCounters typeCounters = entryHolder.getServerTypeDesc().getTypeCounters();
        if (!context.isMemoryOnlyEntry()) {
            internalRDBMS.insertEntry(context, entryHolder);
            typeCounters.incDiskModifyCounter(); // TODO: @sagiv move counters out of SA
            typeCounters.incDiskEntriesCounter();
        }
        if (context.isMemoryOnlyEntry() || context.isMemoryAndDiskEntry()) { // TODO: @sagiv move counters out of SA
            typeCounters.incRamEntriesCounter();
        }
    }

    @Override
    public void updateEntry(Context context, IEntryHolder updatedEntry, boolean updateRedoLog, boolean origin, boolean[] partialUpdateValuesIndicators) throws SAException {
        if (logger.isDebugEnabled()) {
            logger.debug("call updateEntry");
        }
        if (!context.isMemoryOnlyEntry()) {
            internalRDBMS.updateEntry(context, updatedEntry);
            updatedEntry.getServerTypeDesc().getTypeCounters().incDiskModifyCounter();
        }
    }

    @Override
    public void removeEntry(Context context, IEntryHolder entryHolder, boolean origin,
                            boolean fromLeaseExpiration, boolean shouldReplicate) throws SAException {
        if (logger.isDebugEnabled()) {
            logger.debug("call removeEntry");
        }
        if (fromLeaseExpiration) {
            return;
        }
        boolean removed = false;
        if (!context.isMemoryOnlyEntry()) {
            removed = internalRDBMS.removeEntry(context, entryHolder);
        }
        TypeCounters typeCounters = entryHolder.getServerTypeDesc().getTypeCounters();
        if (removed) {// TODO: @sagiv move counters out of SA
            typeCounters.decDiskEntriesCounter();
        }
        if (context.isMemoryOnlyEntry() || context.isMemoryAndDiskEntry()) {
            typeCounters.decRamEntriesCounter();
        }
    }

    @Override
    public void prepare(Context context, ServerTransaction xtn, ArrayList<IEntryHolder> pLocked, boolean singleParticipant, Map<String, Object> partialUpdatesAndInPlaceUpdatesInfo, boolean shouldReplicate) throws SAException {

        if (!singleParticipant) {
            throw new SAException("only single participant transactions are supported");
        }

        HashMap<String, IEntryHolder> entriesForBulkOperations = new HashMap<>();
        //1. locate & setDirty to the relevant entries
        for (IEntryHolder inputeh : pLocked) {
            IEntryHolder entryHolder = inputeh.getOriginalEntryHolder();

            if (entryHolder == null
                    || entryHolder.isDeleted()
                    || !xtn.equals(entryHolder.getWriteLockTransaction())) {
                continue;
            }

            final TieredState entryTieredState = engine.getTieredStorageManager().getEntryTieredState(entryHolder);

            if (TieredState.TIERED_HOT.equals(entryTieredState)) {
                continue;  //nothing to do, dummy op
            }

            entriesForBulkOperations.put(entryHolder.getUID(), entryHolder);
        }

        if (entriesForBulkOperations.isEmpty()) {
            return;  //nothing to do
        }

        //call the underlying RDBMS
        try {
            List<TieredStorageBulkOperationRequest> operations = new ArrayList<>(entriesForBulkOperations.size());
            for (IEntryHolder entryHolder : entriesForBulkOperations.values()) {
                switch (entryHolder.getWriteLockOperation()) {
                    case SpaceOperations.WRITE:
                        operations.add(new TieredStorageInsertBulkOperationRequest(entryHolder));
                        break;
                    case SpaceOperations.UPDATE:
                        operations.add(new TieredStorageUpdateBulkOperationRequest(entryHolder));
                        break;
                    case SpaceOperations.TAKE:
                    case SpaceOperations.TAKE_IE: //todo @sagiv handle phantom
                        operations.add(new TieredStorageRemoveBulkOperationRequest(entryHolder));
                        break;
                    default:
                        throw new UnsupportedOperationException("uid=" + entryHolder.getUID() + " operation=" + entryHolder.getWriteLockOperation());
                }

            }

            List<TieredStorageBulkOperationResult> results = internalRDBMS.executeBulk(operations, xtn);

            //scan and if exception in any result - throw it
            for (TieredStorageBulkOperationResult res : results) {
                if (res.getException() != null)
                    throw res.getException();
            }
        } catch (Throwable t) {
            if (logger.isErrorEnabled()) {
                logger.error("Get exception on prepare: ", t);
            }
            throw new SAException(t);
        }
    }

    @Override
    public void rollback(ServerTransaction xtn, boolean anyUpdates) throws SAException {
    }

    @Override
    public IEntryHolder getEntry(Context context, String uid, String classname, IEntryHolder template) throws SAException {
        if (logger.isDebugEnabled()) {
            logger.debug("call getEntry");
        }

        if (template instanceof  ITemplateHolder){
            ITemplateHolder templateHolder = (ITemplateHolder) template;
            if (templateHolder.isReadOperation()) {
                templateHolder.getServerTypeDesc().getTypeCounters().incDiskReadCounter();
            }
        }
        if (!context.isMemoryOnlyEntry()) {
            return internalRDBMS.getEntryByUID(classname, uid);
        }
        return null;
    }

    @Override
    public Map<String, IEntryHolder> getEntries(Context context, Object[] ids, String typeName, IEntryHolder[] templates) throws SAException {
        return null;
    }

    @Override
    public ISAdapterIterator<IEntryHolder> makeEntriesIter(ITemplateHolder template, long SCNFilter, long leaseFilter, IServerTypeDesc[] subClasses) throws SAException {
        return new MultiTypedRDBMSISIterator(this, subClasses, template);
    }

    @Override
    public void commit(ServerTransaction xtn, boolean anyUpdates) throws SAException {
        //TODO: @sagiv/@tomer PIC-809 right now we call commit in the prepare stage
        try {
            internalRDBMS.closeTransactionConnection(xtn.id);
        } catch (SQLException e) {
            throw new SAException("Failed to close transaction [" + xtn.id + "] in internal RDBMS", e);
        }
    }

    @Override
    public int count(ITemplateHolder template, String[] subClasses) throws SAException {
        return 0;
    }

    @Override
    public void shutDown() throws SAException {
        if (logger.isDebugEnabled()) {
            logger.debug("call shutDown");
        }
        internalRDBMS.shutDown();
        if (externalInitialLoadSA != null)
            externalInitialLoadSA.shutDown();
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public boolean supportsExternalDB() {
        return true;
    }

    @Override
    public boolean supportsPartialUpdate() {
        return false;
    }

    @Override
    public boolean supportsGetEntries() {
        return false;
    }

    @Override
    public void introduceDataType(ITypeDesc typeDesc) {

    }

    @Override
    public SpaceSynchronizationEndpoint getSynchronizationInterceptor() {
        return null;
    }

    @Override
    public Class<?> getDataClass() {
        return null;
    }

    @Override
    public void addIndexes(String typeName, SpaceIndex[] indexes) {
        Arrays.stream(indexes).forEach(index -> {
            try {
                internalRDBMS.createIndex(typeName, index.isUnique(), index.getName());
            } catch (SAException e) {
                logger.error("Could not add index into tiered storage ", e.getMessage());
            }
        });
    }

    public void deleteData() throws SAException {
        if (logger.isDebugEnabled()) {
            logger.debug("call deleteData");
        }
        internalRDBMS.deleteData();
    }

    public void persistType(ITypeDesc typeDesc) throws SAException {
        if (logger.isDebugEnabled()) {
            logger.debug("call persistType");
        }
        internalRDBMS.persistType(typeDesc);
    }

    public void unpersistType(ITypeDesc typeDesc) throws SAException {
        if (logger.isDebugEnabled()) {
            logger.debug("call unpersistType");
        }
        internalRDBMS.unpersistType(typeDesc);
    }

    public SpaceTypeManager getTypeManager() {
        return internalRDBMS.getTypeManager();
    }
}

