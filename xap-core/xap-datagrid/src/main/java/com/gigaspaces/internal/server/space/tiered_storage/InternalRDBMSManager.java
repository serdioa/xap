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
package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.metadata.TypeCounters;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.InitialLoadInfo;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.cache.context.TieredState;
import com.j_spaces.core.sadapter.ISAdapterIterator;
import com.j_spaces.core.sadapter.SAException;

import java.io.IOException;

public class InternalRDBMSManager {

    InternalRDBMS internalRDBMS;

    public InternalRDBMSManager(InternalRDBMS internalRDBMS) {
        this.internalRDBMS = internalRDBMS;
    }

    public boolean initialize(String spaceName, String fullMemberName, SpaceTypeManager typeManager, boolean isBackup) throws SAException{
        return internalRDBMS.initialize(spaceName, fullMemberName, typeManager, isBackup);
    }

    public long getDiskSize() throws SAException, IOException{
        return internalRDBMS.getDiskSize();
    }

    public long getFreeSpaceSize() throws SAException, IOException{
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
        if(isOriginEntryInHotTier != isUpdatedEntryInHotTier){
            //updated enrty moved to HOT - increase ram entries counter
            if(isUpdatedEntryInHotTier){
                typeCounters.incRamEntriesCounter();
            }
            else{ //updated entry moved to COLD - decrease ram entries counter
                typeCounters.decRamEntriesCounter();
            }
        }
    }

    public IEntryHolder getEntryById(Context context, String typeName, Object id, ITemplateHolder templateHolder) throws SAException{
        IEntryHolder entryById = internalRDBMS.getEntryById(context, typeName, id);

        if (templateHolder != null && templateHolder.isReadOperation()){
            templateHolder.getServerTypeDesc().getTypeCounters().incDiskReadCounter();
        }

        return entryById;
    }

    public IEntryHolder getEntryByUID(Context context, String typeName, String uid, ITemplateHolder templateHolder) throws SAException{
        IEntryHolder entryByUID = internalRDBMS.getEntryByUID(context, typeName, uid);

        if (templateHolder != null && templateHolder.isReadOperation()){
            templateHolder.getServerTypeDesc().getTypeCounters().incDiskReadCounter();
        }
        return entryByUID;
    }

    public ISAdapterIterator<IEntryHolder> makeEntriesIter(Context context, String typeName, ITemplateHolder templateHolder) throws SAException{
        ISAdapterIterator<IEntryHolder> iEntryHolderISAdapterIterator = internalRDBMS.makeEntriesIter(context, typeName, templateHolder);

        if (templateHolder != null && templateHolder.isReadOperation() && !context.isDisableTieredStorageMetric()){
            templateHolder.getServerTypeDesc().getTypeCounters().incDiskReadCounter();
        }


        return iEntryHolderISAdapterIterator;
    }

    public boolean isKnownType(String name){
        return internalRDBMS.isKnownType(name);
    }

    public void shutDown(){
        internalRDBMS.shutDown();
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
}

