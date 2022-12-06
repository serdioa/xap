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

package com.j_spaces.core.sadapter;
/*******************************************************************************
 * Copyright (c) 2010 GigaSpaces Technologies Ltd. All rights reserved
 *
 * The software source code is proprietary and confidential information of GigaSpaces. You may use
 * the software source code solely under the terms and limitations of The license agreement granted
 * to you by GigaSpaces.
 *******************************************************************************/

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.metadata.index.SpaceIndex;
import com.gigaspaces.sync.SpaceSynchronizationEndpoint;
import com.j_spaces.core.cache.InitialLoadInfo;
import com.j_spaces.core.cache.context.Context;
import net.jini.core.transaction.server.ServerTransaction;

import java.util.ArrayList;
import java.util.Map;

/**
 * IStorageAdapter is an interface that should be implemented by the Storage adapter.
 *
 * The IStorageAdapter interface contains methods for storing and retrieving entries and notify
 * templates and to provide  storage services to the engine . <p> All IStorageAdapter methods (
 * except init ) gets StorageAdapterContext object as the first parameter, which holds the
 * connection information to the storage-adapter.
 */

public interface IStorageAdapter {
    /**
     * Initializes the SA for work (first API called by engine) <p/> If initMode is WARM : SA is
     * required to set its internal working tables and environment to the state recorded in its
     * persistent storage, and verify that all the SA space-related data is in consistent state.
     * <p/> If initMode is COLD : SA is required to clean its internal state. <p/>
     *
     * @throws SAException - In case a SQL error occurred.
     */
    void initialize() throws SAException;


    ISAdapterIterator initialLoad(Context context, ITemplateHolder template, InitialLoadInfo initialLoadInfo) throws SAException;

    /**
     * Inserts a new entry to the SA storage.
     *
     * @param entryHolder entry to insert
     */
    void insertEntry(Context context, IEntryHolder entryHolder, boolean origin, boolean shouldReplicate) throws SAException;

    /**
     * updates an entry. <p/> <p/>
     *
     * @param updatedEntry new content, same UID and class
     */
    void updateEntry(Context context, IEntryHolder updatedEntry, boolean updateRedoLog, boolean origin,
                     boolean[] partialUpdateValuesIndicators) throws SAException;

    /**
     * Removes an entry from the  SA storage. <p/>
     *
     * @param entryHolder entry to remove
     */
    void removeEntry(Context context, IEntryHolder entryHolder, boolean origin, boolean fromLeaseExpiration, boolean shouldReplicate) throws SAException;

    /**
     * Performs prepare to transaction- write to SA all new entries under the xtn, mark taken
     * entries under the xtn
     *
     * @param xtn               transaction to prepare
     * @param pLocked           a vector of all entries locked (written or taken) under the xtn
     * @param singleParticipant - if true, a transactional SA needs to do all the transaction
     *                          related operations in one DB transaction and the prepare is actually
     *                          the commit (no other transaction related APIs will be called for
     *                          this xtn)
     */
    void prepare(Context context, ServerTransaction xtn, ArrayList<IEntryHolder> pLocked, boolean singleParticipant,
                 Map<String, Object> partialUpdatesAndInPlaceUpdatesInfo, boolean shouldReplicate) throws SAException;

    /**
     * Performs rollback to transaction- rewrite taken entries, remove new entries.
     *
     * @param xtn        transaction to roll-back
     * @param anyUpdates true if any updates performed under tjis xtn
     */
    void rollback(ServerTransaction xtn, boolean anyUpdates) throws SAException;

    /**
     * Gets an entry object from the storage adapter.
     *
     * <p>
     *
     * @param uid       ID of the entry to get
     * @param classname class of the entry to get
     * @param template  selection template,may be null, currently used by cacheload/cachestore in
     *                  order to pass primery key fields when GS uid is not saved in an external DB
     * @return IEntryHolder
     */
    IEntryHolder getEntry(Context context, String uid, String classname, IEntryHolder template) throws SAException;

    /**
     * Gets a map of entries from the storage adapter
     *
     * @param ids       the ids of the entries to get
     * @param typeName  type of the entries to get
     * @param templates templates matching the ids of the entries to get
     * @return A map between entries uid and the entry holder returning null from this method
     * implies this operation is not supported by the underlying storage mechanism and other methods
     * for obtaining these entries should be applied.
     */
    Map<String, IEntryHolder> getEntries(Context context, final Object[] ids, String typeName, IEntryHolder[] templates) throws SAException;

    /**
     * Returns an iterator with entries that match the template, and the other parameters. <p>
     *
     * @param template    -    the entries should match that template.
     * @param SCNFilter   -   if != 0 gets only templates that are <= from SCNFilter.
     * @param leaseFilter if != 0 gets only templates that have expiration_time >= leaseFilter.
     * @param subClasses  -  array of sub classes of the template, the entries should belong to a
     *                    subcalss.
     * @return ISadapterIterator
     **/
    ISAdapterIterator<IEntryHolder> makeEntriesIter(ITemplateHolder template, long SCNFilter, long leaseFilter,
                                                    IServerTypeDesc[] subClasses) throws SAException;

    /**
     * Performs commit to transaction- delete taken entries, commit new entries.
     *
     * @param xtn        transaction to commit
     * @param anyUpdates true if any updates performed under tjis xtn
     */
    void commit(ServerTransaction xtn, boolean anyUpdates) throws SAException;

    /**
     * Returns the number of entries that match the template.
     *
     * @param template   -    the entries should match the template.
     * @param subClasses -  subclassesof the template, the entries can be in one of them.
     * @return int - number of entries that match the parameters.
     */
    int count(ITemplateHolder template, String[] subClasses) throws SAException;

    /**
     * called by the engine after all connections have been closed.
     *
     * if the ShotDownString property is set- the string is executed by the SA
     */
    void shutDown() throws SAException;

    /**
     * If returns true - then the storage adapter is read-only and can't be written to.
     */
    boolean isReadOnly();

    boolean supportsExternalDB();

    boolean supportsPartialUpdate();

    /**
     * Denotes whether the implementing class has an implementation for {@ #getEntries(Object[],
     * String, IEntryHolder[])}
     */
    boolean supportsGetEntries();

    void introduceDataType(ITypeDesc typeDesc);

    SpaceSynchronizationEndpoint getSynchronizationInterceptor();

    Class<?> getDataClass();

    void addIndexes(String typeName, SpaceIndex[] indexes) throws SAException;
}