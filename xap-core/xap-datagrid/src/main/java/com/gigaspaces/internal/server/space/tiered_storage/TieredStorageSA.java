package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.metadata.TypeCounters;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.gigaspaces.internal.server.space.tiered_storage.transaction.*;
import com.gigaspaces.internal.server.storage.EntryHolder;
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
        if ((!context.isMemoryOnlyEntry())) { // TODO: @sagiv fix it...
            internalRDBMS.insertEntry(context, entryHolder);
            typeCounters.incDiskModifyCounter();
            typeCounters.incDiskEntriesCounter();
        }
        if (context.isMemoryOnlyEntry() || context.isMemoryAndDiskEntry()) { // TODO: @sagiv fix it...
            typeCounters.incRamEntriesCounter();
        }
    }

    @Override
    public void updateEntry(Context context, IEntryHolder updatedEntry, boolean updateRedoLog, boolean origin, boolean[] partialUpdateValuesIndicators) throws SAException {
        if (logger.isDebugEnabled()) {
            logger.debug("call updateEntry");
        }
        internalRDBMS.updateEntry(context, updatedEntry);
        updatedEntry.getServerTypeDesc().getTypeCounters().incDiskModifyCounter();
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
        if (!context.isMemoryOnlyEntry()) { // TODO: @sagiv fix it...
            removed = internalRDBMS.removeEntry(context, entryHolder);
        }
        TypeCounters typeCounters = entryHolder.getServerTypeDesc().getTypeCounters();
        if (removed) { // TODO: @sagiv fix it...
            typeCounters.decDiskEntriesCounter();
        }
        if (context.isMemoryOnlyEntry() || context.isMemoryAndDiskEntry()) {
            typeCounters.decRamEntriesCounter();
        }
    }

    @Override
    public void prepare(Context context, ServerTransaction xtn, ArrayList<IEntryHolder> pLocked, boolean singleParticipant, Map<String, Object> partialUpdatesAndInPlaceUpdatesInfo, boolean shouldReplicate) throws SAException {

        HashMap<String, IEntryHolder> ohEntries = new HashMap<>();
        List<String> uids = (shouldReplicate && engine.getReplicationNode().getDirectPesistencySyncHandler() != null) ?
                new ArrayList<>(pLocked.size()) : null;
        Set<String> takes = null;
        boolean checkedOnTake = false;
        //1. locate & setDirty to the relevant entries
        for (IEntryHolder inputeh : pLocked) {
            IEntryHolder entryHolder = inputeh.getOriginalEntryHolder();

//            if (!entryHolder.isBlobStoreEntry())
//                continue;
            if (entryHolder == null
                    || entryHolder.isDeleted()
                    || entryHolder.getWriteLockTransaction() == null
                    || !entryHolder.getWriteLockTransaction().equals(xtn)) {
                continue;
            }

            final TieredState entryTieredState = engine.getTieredStorageManager().getEntryTieredState(entryHolder);

//            if ((entryHolder.getWriteLockOperation() == SpaceOperations.TAKE
//                    || entryHolder.getWriteLockOperation() == SpaceOperations.TAKE_IE)
//                    &&
//                    !((IBlobStoreEntryHolder) entryHolder).getBlobStoreResidentPart().isInBlobStoreStorage())
            if (TieredState.TIERED_HOT.equals(entryTieredState)) {
                continue;  //nothing to do, dummy op
            }

            //setdirty will also prepare for flushing
//            ((IBlobStoreEntryHolder) entryHolder).setDirty(engine.getCacheManager());
            ohEntries.put(entryHolder.getUID(), entryHolder);
            if (entryHolder.getWriteLockOperation() == SpaceOperations.TAKE
                    || entryHolder.getWriteLockOperation() == SpaceOperations.TAKE_IE) {
                if (!checkedOnTake) {
                    if (useEmbeddedSyncOnPrimary(useDirectPersistencySync(shouldReplicate))) {
                        takes = new HashSet<>();
                    }
                    checkedOnTake = true;
                }
                if (takes != null) {
                    takes.add(entryHolder.getUID());
                }
            }
            if (uids != null) {
                uids.add(entryHolder.getUID());
            }
        }

        if (ohEntries.isEmpty()) {
            return;  //nothing to do
        }

        boolean needDirectPersistencySync = false;
        if (uids != null) {
            needDirectPersistencySync = handleDirectPersistencyConsistency(context, uids, shouldReplicate, takes, ohEntries);
        }

        //call the underlying RDBMS
        try {
            List<TieredStorageBulkOperationRequest> operations = new ArrayList<>();
            for (IEntryHolder entryHolder : ohEntries.values()) {
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
//                        boolean phantom = ((IBlobStoreEntryHolder) entryHolder).isPhantom();
//                        if (!phantom) { //actual remove
//                            operations.add(new TieredStorageRemoveBulkOperationRequest(entryCacheInfo.getStorageKey(), entryCacheInfo.getBlobStoreStoragePos(), entryCacheInfo));
//                        } else //update
//                            operations.add(new TieredStorageUpdateBulkOperationRequest(entryCacheInfo.getStorageKey(),
//                                    entryCacheInfo.getEntryLayout(engine.getCacheManager()), entryCacheInfo.getBlobStoreStoragePos(), entryCacheInfo));
                        break;
                    default:
                        throw new UnsupportedOperationException("uid=" + entryHolder.getUID() + " operation=" + entryHolder.getWriteLockOperation());
                }

            }

//            List<BlobStoreBulkOperationResult> results = engine.getCacheManager().getBlobStoreStorageHandler().executeBulk(operations, BlobStoreObjectType.DATA, true/*transactional*/);
            List<TieredStorageBulkOperationResult> results = new ArrayList<>();
            if (singleParticipant) {
                results = internalRDBMS.executeBulk(operations, xtn);
            }
            //scan and if execption in any result- throw it
            for (TieredStorageBulkOperationResult res : results) {
                if (res.getException() != null)
                    throw res.getException();
            }

            for (TieredStorageBulkOperationResult res : results) {
//                IEntryHolder entryHolder = ohEntries.get(res.getId());

                //for each entry in result list perform setDirty to false, and set the BlobStoreStoragePosition if applicable
//                switch (entryHolder.getWriteLockOperation()) {
//                    case SpaceOperations.WRITE:
//                    case SpaceOperations.UPDATE:
//                        ((IBlobStoreEntryHolder) entryHolder).getBlobStoreResidentPart().flushedFromBulk(context, engine.getCacheManager(), res.getPosition(), false/*removed*/, true /*onXtnEnd*/);
//                        break;
//                    case SpaceOperations.TAKE:
//                    case SpaceOperations.TAKE_IE:
//                        ((IBlobStoreEntryHolder) entryHolder).getBlobStoreResidentPart().flushedFromBulk(context, engine.getCacheManager(), null, true/*removed*/, true /*onXtnEnd*/);
//                        break;
//                }
            }
        } catch (Throwable t) {
            logger.error(getClass().getName() + " Prepare " + t);
            throw new SAException(t);
        }

        if (needDirectPersistencySync)
            reportOpPersisted(context);
    }

    @Override
    public void rollback(ServerTransaction xtn, boolean anyUpdates) throws SAException {
    }

    @Override
    public IEntryHolder getEntry(Context context, String uid, String classname, IEntryHolder template) throws SAException {
        if (logger.isDebugEnabled()) {
            logger.debug("call getEntry");
        }
        boolean isMaybeUnderTransaction = false;
        if (template instanceof  ITemplateHolder){
            isMaybeUnderTransaction = template.isMaybeUnderXtn();
        } else if (template instanceof EntryHolder){
            CachePredicate cachePredicate = engine.getTieredStorageManager().getCacheRule(template.getClassName());
            if (cachePredicate != null && (cachePredicate.isTimeRule())){
                isMaybeUnderTransaction = template.isMaybeUnderXtn();
            } else if (template.getEntryData() != null ) {
                isMaybeUnderTransaction = template.getEntryData().getExpirationTime() < Long.MAX_VALUE;
            }
        }
        IEntryHolder entryByUID = internalRDBMS.getEntryByUID(classname, uid, isMaybeUnderTransaction);
        if (template instanceof ITemplateHolder) {
            ITemplateHolder templateHolder = (ITemplateHolder) template;
            if (templateHolder.isReadOperation()) {
                templateHolder.getServerTypeDesc().getTypeCounters().incDiskReadCounter();
            }
        }
        return entryByUID;
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
        try {
            internalRDBMS.closeConnection(xtn.id); //TODO: @sagiv we should call here to commit?
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

    private boolean useDirectPersistencySync(boolean shouldReplicate) {
        return (shouldReplicate && engine.getReplicationNode().getDirectPesistencySyncHandler() != null && engine.getSpaceImpl().isPrimary());
    }

    private boolean useEmbeddedSyncOnPrimary(boolean directPersistencySyncUsed) {
        return (directPersistencySyncUsed && engine.getReplicationNode().getDirectPesistencySyncHandler().isEmbeddedListUsed());
    }

    //handle consistency between primary and backup in case of reloading
    private boolean handleDirectPersistencyConsistency(Context context, List<String> uids, boolean shouldReplicate, Set<String> removedUids, Map<String, IEntryHolder> entryHolderMap) {
        if (!useDirectPersistencySync(shouldReplicate))
            return false;
        if (logger.isDebugEnabled()) {
            logger.debug("[" + engine.getFullSpaceName() + "] handling adding entries with uids=" + uids + " to sync using InternalRDBMSManager");
        }
        engine.getReplicationNode().getDirectPesistencySyncHandler().beforeDirectPersistencyOp(engine.getCacheManager().getReplicationContext(context), uids, removedUids, entryHolderMap);
        return true;
    }

    private void reportOpPersisted(Context context) {
        engine.getReplicationNode().getDirectPesistencySyncHandler().afterOperationPersisted(context.getReplicationContext().getDirectPersistencyPendingEntry());
    }

}

