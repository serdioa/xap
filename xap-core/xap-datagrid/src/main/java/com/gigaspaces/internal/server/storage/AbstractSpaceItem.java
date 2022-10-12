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

package com.gigaspaces.internal.server.storage;

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.metadata.TypeDescriptorUtils;
import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.space.SpaceUidFactory;
import com.gigaspaces.internal.transport.CompoundRoutingHashValue;
import com.gigaspaces.internal.utils.Textualizable;
import com.gigaspaces.internal.utils.Textualizer;
import com.j_spaces.core.Constants;
import com.j_spaces.core.client.NotifyModifiers;
import com.j_spaces.kernel.locks.IEvictableLockObject;
import org.slf4j.Logger;

import java.rmi.MarshalledObject;
import java.util.List;

/**
 * @author Niv Ingberg
 * @since 7.0
 */
public abstract class AbstractSpaceItem implements ISpaceItem, Textualizable {
    private final transient IServerTypeDesc _typeDesc;

    /**
     * Store Entry Unique ID. If this field is not <b>null</b> then this UID will be used by the
     * Space, otherwise the space will generate it automatically. When entry have all its fields
     * null (null template) and its UID is assigned, matching will be done using the UID only.
     *
     * The UID is a String based identifier and composed of the following parts: - Class information
     * class Hashcode and name size - Space node name At clustered environment combined from
     * container-name :space name. At non-clustered environment combined from dummy name. -
     * Timestamp - Counter
     */
    private String _uid;
    /**
     * inserting timestamp.
     */
    private long _scn;
    /**
     * A transient entry in a persistent space resides in memory and is not written to the DB.
     */
    private boolean _transient;

    private transient volatile byte flags;

    /**
     * inserting order within same time stamp.
     */
    private int _order;

    protected AbstractSpaceItem(IServerTypeDesc typeDesc, String uid,
                                long scn, boolean isTransient) {
        this._typeDesc = typeDesc;
        this._uid = uid;
        this._scn = scn;
        this._transient = isTransient;
    }

    protected AbstractSpaceItem(IEntryHolder other) {
        this(other.getServerTypeDesc(), other.getUID(), other.getSCN(), other.isTransient());
    }

    @Override
    public IServerTypeDesc getServerTypeDesc() {
        return _typeDesc;
    }

    @Override
    public String getClassName() {
        return _typeDesc.getTypeName();
    }

    @Override
    public String getUID() {
        return _uid;
    }

    public void setUID(String uid) {
        this._uid = uid;
    }

    @Override
    public long getSCN() {
        return _scn;
    }

    public void setSCN(long scn) {
        this._scn = scn;
    }

    public long getExpirationTime() {
        return getEntryData().getExpirationTime();
    }

    @Override
    public abstract void setExpirationTime(long expirationTime);

    public int getVersionID() {
        return getEntryData().getVersion();
    }

    @Override
    public boolean isTransient() {
        return _transient;
    }

    public void setTransient(boolean _transient) {
        this._transient = _transient;
    }

    @Override
    public boolean isMaybeUnderXtn() {
        return getFlag(Constants.SpaceItem.IS_MAYBE_UNDER_TRANSACTION);
    }

    @Override
    public void setMaybeUnderXtn(boolean maybeUnderXtn) {
        setFlag(Constants.SpaceItem.IS_MAYBE_UNDER_TRANSACTION, maybeUnderXtn);
    }

    @Override
    public boolean hasWaitingFor() {
        return getFlag(Constants.SpaceItem.HAS_WAITING_FOR);
    }

    @Override
    public void setHasWaitingFor(boolean hasWaitingFor) {
        setFlag(Constants.SpaceItem.HAS_WAITING_FOR, hasWaitingFor);
    }

    @Override
    public boolean isDeleted() {
        return getFlag(Constants.SpaceItem.IS_DELETED);
    }

    @Override
    public void setDeleted(boolean isDeleted) {
        setFlag(Constants.SpaceItem.IS_DELETED, isDeleted);
    }

    private boolean getFlag(byte mask) {
        return (flags & mask) > 0; // bit is 1
    }

    //NOTE - to be called under lock
    private void setFlag(byte mask, boolean value) {
        if (value) {
            //set true (set the bit to 1)
            flags |= mask;
        } else {
            //set false (set the bit to 0)
            flags &= ~mask;
        }
    }

    @Override
    public boolean isDummyLease() {
        IEntryData entryData = getEntryData();
        return entryData != null && entryData.isDummyLease();
    }

    public boolean isDummyLeaseAndNotExpired() {
        return isDummyLease() && isMaybeUnderXtn();
    }


    public void setDummyLease() {
        setExpirationTime(Constants.TieredStorage.DUMMY_LEASE_FOR_TRANSACTION);
    }

    public void dump(Logger logger, String msg) {
        logger.info(msg);
        logger.info("Start Dumping " + getClass().getName());

        logger.info("Class Name: " + getClassName());
        logger.info("UID: " + getUID());
        logger.info("SCN: " + getSCN());
        logger.info("Transient: " + isTransient());
        logger.info("Expiration Time : " + getExpirationTime());
        logger.info("Deleted: " + isDeleted());
        logger.info("MaybeUnderXtn: " + isMaybeUnderXtn());
        logger.info("HasWaitingFor: " + hasWaitingFor());
    }

    /*******************************
     * ISelfLockingSubject Members *
     ******************************/

    /**
     * if entryHolder is used as lockObject (for example in ALL_IN_CACHE) - its the lock subject
     * itself
     *
     * @return true if is the subject itself
     */
    public boolean isLockSubject() {
        return true;
    }

    /**
     * if the lock object is an evictable lock object, return the interface (preferable to casing)
     *
     * @return IEvictableLockObject if implemented
     */
    public IEvictableLockObject getEvictableLockObject() {
        // Not relevant for entryHolder as a lock object
        return null;
    }

    /************************
     * IEntryHolder Members *
     ************************/

    public boolean isShadow() {
        return false;
    }

    // Return true if this entry has a shadow entry
    public boolean hasShadow() {
        return hasShadow(false /*safeEntry*/);
    }

    public abstract boolean hasShadow(boolean safeEntry);


    public int getNotifyType() {
        return NotifyModifiers.NOTIFY_NONE;
    }

    public MarshalledObject getHandback() {
        return null;
    }

    public int getOrder() {
        return _order;
    }

    public void setOrder(int order) {
        this._order = order;
    }

    public Object getRoutingValue() {

        IEntryData edata = getEntryData(); //similar code to abstractEntryPacket getRoutingValue()
        if (edata.getNumOfFixedProperties() == 0)
            return null;

        ITypeDesc typeDesc = edata.getEntryTypeDesc().getTypeDesc();
        List<String> routingProperties = typeDesc.getIdPropertiesNames();

        if (typeDesc.isAutoGenerateRouting()) {
            return SpaceUidFactory.extractPartitionId(getUID());
        }
        int routingPropertyId = typeDesc.getRoutingPropertyId();
        if (routingProperties == null || routingPropertyId == -1) return null;

        if (typeDesc.hasRoutingAnnotation() || routingProperties.size() == 1) {
            return edata.getFixedPropertyValue(routingPropertyId);
        }
        CompoundRoutingHashValue result = new CompoundRoutingHashValue();
        for (String propertyName : routingProperties) {
            Object propertyValue = edata.getPropertyValue(propertyName);
            if (propertyValue == null) { //if one of field values is null, perform broadcast
                return null;
            }
            result.concatValue(propertyValue);

        }
        return result;
    }

    public Object getEntryId() {
        IEntryData entryData = getEntryData();
        ITypeDesc typeDesc = entryData.getEntryTypeDesc().getTypeDesc();
        if (typeDesc.isAutoGenerateId())
            return getUID();

        int[] identifierPropertiesId = typeDesc.getIdentifierPropertiesId();
        if (identifierPropertiesId.length == 0)
            return null;

        return TypeDescriptorUtils.toSpaceId(identifierPropertiesId, entryData::getFixedPropertyValue);
    }

    @Override
    public void toText(Textualizer textualizer) {
        textualizer.append("typeName", getClassName());
        textualizer.append("uid", getUID());
    }

    @Override
    public String toString() {
        return Textualizer.toString(this);
    }
}
