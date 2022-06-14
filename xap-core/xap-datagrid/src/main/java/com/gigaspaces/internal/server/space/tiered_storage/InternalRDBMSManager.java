package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.metadata.TypeCounters;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.metadata.index.SpaceIndex;
import com.gigaspaces.sync.SpaceSynchronizationEndpoint;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.InitialLoadInfo;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.cache.context.TieredState;
import com.j_spaces.core.sadapter.ISAdapterIterator;
import com.j_spaces.core.sadapter.IStorageAdapter;
import com.j_spaces.core.sadapter.SAException;
import net.jini.core.transaction.server.ServerTransaction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

public class InternalRDBMSManager implements IStorageAdapter {

    private final InternalRDBMS internalRDBMS;
    private IStorageAdapter externalInitialLoadSA;
    private boolean loadedFromMirror = false;

    public InternalRDBMSManager(InternalRDBMS internalRDBMS) {
        this.internalRDBMS = internalRDBMS;
    }

    public void setExternalInitialLoadSA(IStorageAdapter externalInitialLoadSA) {
        this.externalInitialLoadSA = externalInitialLoadSA;
    }

    public boolean initialize(String spaceName, String fullMemberName, SpaceTypeManager typeManager, boolean isBackup) throws SAException {
        return internalRDBMS.initialize(spaceName, fullMemberName, typeManager, isBackup);
    }

    public long getDiskSize() throws SAException, IOException {
        return internalRDBMS.getDiskSize();
    }

    public long getFreeSpaceSize() throws SAException, IOException {
        return internalRDBMS.getFreeSpaceSize();
    }

    public void createTable(ITypeDesc typeDesc) throws SAException{
        internalRDBMS.createTable(typeDesc);
    }

    public void dropTable(ITypeDesc typeDesc) throws SAException{
        internalRDBMS.dropTable(typeDesc);
    }

    /**
     * Inserts a new entry to the internalDiskStorage
     *
     * @param entryHolder entry to insert
     * @param initialLoadOrigin
     */
    public void insertEntry(Context context, IEntryHolder entryHolder, CacheManager.InitialLoadOrigin initialLoadOrigin) throws SAException{
        TypeCounters typeCounters = entryHolder.getServerTypeDesc().getTypeCounters();
        if(initialLoadOrigin != CacheManager.InitialLoadOrigin.FROM_TIERED_STORAGE && (context.isDiskOnlyEntry() || context.isMemoryAndDiskEntry()) && entryHolder.getXidOriginatedTransaction() == null) {
            internalRDBMS.insertEntry(context, entryHolder);
            typeCounters.incDiskModifyCounter();
            typeCounters.incDiskEntriesCounter();
        }
        if (context.isMemoryOnlyEntry() || context.isMemoryAndDiskEntry()) {
            typeCounters.incRamEntriesCounter();
        }
    }

    /**
     * updates an entry.
     *
     * @param updatedEntry new content, same UID and class
     */
    public void updateEntry(Context context, IEntryHolder updatedEntry) throws SAException{
        internalRDBMS.updateEntry(context, updatedEntry);
        updatedEntry.getServerTypeDesc().getTypeCounters().incDiskModifyCounter();
    }

    /**
     * Removes an entry from the  internalDiskStorage
     *
     * @param entryHolder entry to remove
     */
    public boolean removeEntry(Context context, IEntryHolder entryHolder) throws SAException{
        boolean removed = false;
        if(context.getEntryTieredState() != TieredState.TIERED_HOT) {
            removed = internalRDBMS.removeEntry(context, entryHolder);
        }
        TypeCounters typeCounters = entryHolder.getServerTypeDesc().getTypeCounters();
        String type = entryHolder.getServerTypeDesc().getTypeName();
        if(removed || context.isMemoryOnlyEntry()){
            typeCounters.decDiskEntriesCounter();
            if(context.isMemoryOnlyEntry() || context.isMemoryAndDiskEntry()){
                typeCounters.decRamEntriesCounter();
            }
        }
        return removed;
    }



    public void updateRamEntriesCounterAfterTierChange(String type, boolean isUpdatedEntryInHotTier, boolean isOriginEntryInHotTier){
        TypeCounters typeCounters = getTypeManager().getServerTypeDesc(type).getTypeCounters();
        //check if the tier of the entry changed due to the update operation
        if (isOriginEntryInHotTier != isUpdatedEntryInHotTier) {
            //updated enrty moved to HOT - increase ram entries counter
            if (isUpdatedEntryInHotTier) {
                typeCounters.incRamEntriesCounter();
            } else { //updated entry moved to COLD - decrease ram entries counter
                typeCounters.decRamEntriesCounter();
            }
        }
    }

    public IEntryHolder getEntryByUID(String typeName, String uid, ITemplateHolder templateHolder) throws SAException {
        IEntryHolder entryByUID = internalRDBMS.getEntryByUID(typeName, uid);
        if (templateHolder != null && templateHolder.isReadOperation()) { //TODO: @sagiv !context.isDisableTieredStorageMetric()?
            templateHolder.getServerTypeDesc().getTypeCounters().incDiskReadCounter();
        }
        return entryByUID;
    }

    public ISAdapterIterator<IEntryHolder> makeEntriesIter(String typeName, ITemplateHolder templateHolder) throws SAException {
        ISAdapterIterator<IEntryHolder> iEntryHolderISAdapterIterator = internalRDBMS.makeEntriesIter(typeName, templateHolder);

        if (templateHolder != null && templateHolder.isReadOperation()) {
            templateHolder.getServerTypeDesc().getTypeCounters().incDiskReadCounter();
        }

        return iEntryHolderISAdapterIterator;
    }

    public boolean isKnownType(String name) {
        return internalRDBMS.isKnownType(name);
    }

    @Override
    public void initialize() throws SAException {
        if (externalInitialLoadSA != null)
            externalInitialLoadSA.initialize();
    }

    @Override
    public ISAdapterIterator initialLoad(Context context, ITemplateHolder template) throws SAException {
        if (!loadedFromMirror && externalInitialLoadSA != null) {
            loadedFromMirror = true;
            return externalInitialLoadSA.initialLoad(context, template);
        }
        return null;
    }

    @Override
    public void insertEntry(Context context, IEntryHolder entryHolder, boolean origin, boolean shouldReplicate) throws SAException {

    }

    @Override
    public void updateEntry(Context context, IEntryHolder updatedEntry, boolean updateRedoLog, boolean origin, boolean[] partialUpdateValuesIndicators) throws SAException {

    }

    @Override
    public void removeEntry(Context context, IEntryHolder entryHolder, boolean origin, boolean fromLeaseExpiration, boolean shouldReplicate) throws SAException {

    }

    @Override
    public void prepare(Context context, ServerTransaction xtn, ArrayList<IEntryHolder> pLocked, boolean singleParticipant, Map<String, Object> partialUpdatesAndInPlaceUpdatesInfo, boolean shouldReplicate) throws SAException {

    }

    @Override
    public void rollback(ServerTransaction xtn, boolean anyUpdates) throws SAException {

    }

    @Override
    public IEntryHolder getEntry(Context context, String uid, String classname, IEntryHolder template) throws SAException {
        if (template instanceof ITemplateHolder) {
            return getEntryByUID(classname, uid, (ITemplateHolder) template);
        }
        return getEntryByUID(classname, uid, null);
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

    }

    @Override
    public int count(ITemplateHolder template, String[] subClasses) throws SAException {
        return 0;
    }

    @Override
    public void shutDown() throws SAException {
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
        internalRDBMS.deleteData();
    }

    public void persistType(ITypeDesc typeDesc) throws SAException {
        internalRDBMS.persistType(typeDesc);
    }

    public void unpersistType(ITypeDesc typeDesc) throws SAException {
        internalRDBMS.unpersistType(typeDesc);
    }

    public void initialLoad(Context context, SpaceEngine engine, InitialLoadInfo initialLoadInfo) throws SAException {
        internalRDBMS.initialLoad(context, engine, initialLoadInfo);
    }

    public SpaceTypeManager getTypeManager() {
        return internalRDBMS.getTypeManager();
    }

    //TODO: @sagiv count API
}

