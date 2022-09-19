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

import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.j_spaces.core.XtnEntry;
import com.j_spaces.core.server.transaction.EntryXtnInfo;
import com.j_spaces.kernel.locks.ILockObject;
import net.jini.core.transaction.server.ServerTransaction;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


/**
 * This class represents an entry in a GigaSpace. Each instance of
 * this class contains a reference to the Entry value plus any other necessary info about the entry;
 * including its class name, field types, and field values (could be in a {@link
 * java.rmi.MarshalledObject MarshalledObject} form).
 */

@com.gigaspaces.api.InternalApi
public class EntryHolder extends AbstractSpaceItem implements IEntryHolder {

    /**
     * contains all the entry mutable fields, replaced on every change
     */
    private ITransactionalEntryData _entryData;

    //entry is being inserted/updated, in the process of fields insertion
    //NOTE- currently not volatile,
    private boolean _unStable;


    public EntryHolder(IServerTypeDesc typeDesc, String uid, long scn,
                       boolean isTransient, ITransactionalEntryData entryData) {
        super(typeDesc, uid, scn, isTransient);
        if (entryData != null) {
            _entryData = entryData;
            this.setMaybeUnderXtn(entryData.getXidOriginated() != null);
        }
    }

    protected EntryHolder(IEntryHolder other) {
        super(other);
        //NOTE- we dont clone xtn info fields
        _entryData = other.getTxnEntryData().createCopyWithoutTxnInfo();
    }

    @Override
    public IEntryHolder createCopy() {
        return new EntryHolder(this);
    }

    @Override
    public IEntryHolder createDummy() {
        ITransactionalEntryData ed = new FlatEntryData(
                new Object[getEntryData().getNumOfFixedProperties()],
                null,
                getEntryData().getEntryTypeDesc(),
                1 /*versionID*/,
                Long.MAX_VALUE, /* expirationTime */
                null);
        EntryHolder dummy = new EntryHolder(this.getServerTypeDesc(), this.getUID(), this.getSCN(),
                this.isTransient(), ed);
        dummy.setDeleted(true);
        return dummy;
    }

    @Override
    public boolean isUnstable() {
        return _unStable;
    }

    @Override
    public void setunStable(boolean value) {
        this._unStable = value;
    }

    @Override
    public IEntryData getEntryData() {
        return _entryData;
    }

    @Override
    public ITransactionalEntryData getTxnEntryData() {
        return _entryData;
    }

    @Override
    public void updateVersionAndExpiration(int versionID, long expiration) {
        _entryData = _entryData.createCopyWithTxnInfo(versionID, expiration);
    }

    @Override
    public void updateEntryData(IEntryData newEntryData, long expirationTime) {
        _entryData = _entryData.createCopy(newEntryData, expirationTime);
    }

    @Override
    public void setExpirationTime(long expirationTime) {
        ITransactionalEntryData ed = _entryData;
        if (ed.getExpirationTime() == expirationTime)
            return; //save cloning
        _entryData = ed.createCopyWithTxnInfo(ed.getVersion(), expirationTime);
    }

    @Override
    // NOTE - should be called under lock
    public void resetEntryXtnInfo() {
        _entryData = _entryData.createCopyWithoutTxnInfo();
        setMaybeUnderXtn(false);
        setHasWaitingFor(false);
    }

    /**
     */
    @Override
    public void resetWriteLockOwner() {
        if (_entryData.getWriteLockOwner() == null)
            return;

        EntryXtnInfo ex = new EntryXtnInfo(_entryData.getEntryXtnInfo());
        ex.setWriteLockOwner(null);
        _entryData = _entryData.createCopyWithSuppliedTxnInfo(ex);
    }

    @Override
    public void setWriteLockOwnerAndOperation(XtnEntry writeLockOwner, int writeLockOperation) {
        setWriteLockOwnerAndOperation(writeLockOwner, writeLockOperation, true/*createsnapshot*/);
    }

    @Override
    public void setWriteLockOwnerAndOperation(XtnEntry writeLockOwner, int writeLockOperation, boolean createSnapshot) {
        if (!createSnapshot && _entryData.getEntryXtnInfo() == null)
            createSnapshot = true;

        if (!createSnapshot) {
            ITransactionalEntryData ed = _entryData;
            ed.setWriteLockOwner(writeLockOwner);
            ed.setWriteLockOperation(writeLockOperation);
        } else {
            EntryXtnInfo ex = EntryXtnInfo.createCloneOrEmptyInfo(_entryData.getEntryXtnInfo());
            ex.setWriteLockOwner(writeLockOwner);
            ex.setWriteLockOperation(writeLockOperation);
            _entryData = _entryData.createCopyWithSuppliedTxnInfo(ex);
        }
    }

    @Override
    public void setWriteLockOwnerOperationAndShadow(XtnEntry writeLockOwner, int writeLockOperation, IEntryHolder otherEh) {
        EntryXtnInfo ex = EntryXtnInfo.createCloneOrEmptyInfo(_entryData.getEntryXtnInfo());
        ex.setWriteLockOwner(writeLockOwner);
        ex.setWriteLockOperation(writeLockOperation);
        ex.setOtherUpdateUnderXtnEntry(otherEh);
        _entryData = _entryData.createCopyWithSuppliedTxnInfo(ex);
    }

    @Override
    public void restoreUpdateXtnRollback(IEntryData entryData) {
        ITransactionalEntryData newed = _entryData.createCopy(entryData, entryData.getExpirationTime());
        EntryXtnInfo ex = null;
        if (((ITransactionalEntryData) entryData).getEntryXtnInfo() != null)
            ex = new EntryXtnInfo(_entryData.getEntryXtnInfo());
        else
            ex = new EntryXtnInfo();
        ex.setOtherUpdateUnderXtnEntry(null);
        _entryData = newed.createCopyWithSuppliedTxnInfo(ex);


    }

    @Override
    public void setWriteLockOperation(int writeLockOperation, boolean createSnapshot) {
        if (!createSnapshot && _entryData.getEntryXtnInfo() == null)
            createSnapshot = true;
        if (createSnapshot) {
            EntryXtnInfo ex = EntryXtnInfo.createCloneOrEmptyInfo(_entryData.getEntryXtnInfo());
            ex.setWriteLockOperation(writeLockOperation);
            _entryData = _entryData.createCopyWithSuppliedTxnInfo(ex);
        } else
            _entryData.setWriteLockOperation(writeLockOperation);
    }

    /**
     */
    @Override
    public void resetXidOriginated() {
        //	no need to duplicate data for this parameter
        if (_entryData.getEntryXtnInfo() != null)
            _entryData.setXidOriginated(null);
    }

    @Override
    public XtnEntry getXidOriginated() {
        return _entryData.getXidOriginated();
    }

    /**
     * @return the m_XidOriginated transaction
     */
    @Override
    public ServerTransaction getXidOriginatedTransaction() {
        return _entryData.getXidOriginatedTransaction();
    }

    @Override
    public void setOtherUpdateUnderXtnEntry(IEntryHolder eh) {
        if (eh == null && _entryData.getEntryXtnInfo() == null)
            return;
        EntryXtnInfo ex = EntryXtnInfo.createCloneOrEmptyInfo(_entryData.getEntryXtnInfo());
        ex.setOtherUpdateUnderXtnEntry(eh);
        _entryData = _entryData.createCopyWithSuppliedTxnInfo(ex);
    }

    public boolean isUnderPendingUpdate() {
        return isShadow() || hasShadow();
    }

    @Override
    public String getUidToOperateBy() {
        return getUID();
    }

    @Override
    public void dump(Logger logger, String msg) {
        super.dump(logger, msg);

        logger.info("Order: " + getOrder());

        int numOfFields = _entryData.getNumOfFixedProperties();
        for (int pos = 0; pos < numOfFields; pos++)
            logger.info("Object " + _entryData.getFixedPropertyValue(pos));

        logger.info("WriteLockOwner : " + getWriteLockTransaction());

        List<XtnEntry> rlo = getReadLockOwners();
        if (rlo != null)
            for (int pos = 0; pos < getReadLockOwners().size(); pos++)
                logger.info("ReadLockOwners: " + rlo.get(pos));

        logger.info("WriteLockOperation : " + getWriteLockOperation());
        logger.info("XidOriginated : " + getXidOriginatedTransaction());
    }

    @Override
    public boolean anyReadLockXtn() {
        // Get local reference (volatile):
        ITransactionalEntryData entryData = _entryData;
        return entryData.anyReadLockXtn();
    }

    /***
     * @return Returns ReadWriteLock Transaction's Owner Lists.
     **/
    @Override
    public List<XtnEntry> getReadLockOwners() {
        // Get local reference (volatile):
        ITransactionalEntryData entryData = _entryData;
        return entryData.getReadLocksOwners();
    }

    @Override
    public void addReadLockOwner(XtnEntry xtn) {
        if (_entryData.getEntryXtnInfo() == null)
            _entryData = _entryData.createCopyWithTxnInfo(true /*createEmptyTxnInfoIfNone*/);
        // Get local reference (volatile):
        ITransactionalEntryData entryData = _entryData;
        // No need to duplicate data for read locks
        entryData.addReadLockOwner(xtn);
    }

    @Override
    public void removeReadLockOwner(XtnEntry xtn) {
        if (_entryData.getEntryXtnInfo() == null)
            return;
        // Get local reference (volatile):
        ITransactionalEntryData entryData = _entryData;
        // No need to duplicate data for read locks
        entryData.removeReadLockOwner(xtn);
    }

    @Override
    public void clearReadLockOwners() {
        if (_entryData.getEntryXtnInfo() == null)
            return;
        // Get local reference (volatile):
        ITransactionalEntryData entryData = _entryData;
        // No need to duplicate data for read locks
        entryData.clearReadLockOwners();
    }

    @Override
    public XtnEntry getWriteLockOwner() {
        // Get local reference (volatile):
        ITransactionalEntryData entryData = _entryData;
        return entryData.getWriteLockOwner();
    }

    @Override
    public boolean isEntryUnderWriteLockXtn() {
        return getWriteLockOwner() != null;
    }

    @Override
    public int getWriteLockOperation() {
        // Get local reference (volatile):
        ITransactionalEntryData entryData = _entryData;
        return entryData.getWriteLockOperation();
    }

    @Override
    public ServerTransaction getWriteLockTransaction() {
        // Get local reference (volatile):
        ITransactionalEntryData entryData = _entryData;
        return entryData.getWriteLockTransaction();
    }

    private void initWaitingFor() {
        if (_entryData.getEntryXtnInfo() == null)
            _entryData = _entryData.createCopyWithTxnInfo(true /*createEmptyTxnInfoIfNone*/);
        // Get local reference (volatile):
        ITransactionalEntryData entryData = _entryData;
        //NOTE!! - we dont duplicate entryData for waiting-for changes
        entryData.initWaitingFor();
    }

    /**
     * @return Returns the m_WaitingFor. holds all entries waited by templates
     * ReadIfExists/TaleIfExists
     */
    @Override
    public Collection<ITemplateHolder> getTemplatesWaitingForEntry() {
        // Get local reference (volatile):
        ITransactionalEntryData entryData = _entryData;
        return entryData.getWaitingFor();
    }

    @Override
    public Collection<ITemplateHolder> getCopyOfTemplatesWaitingForEntry() {
        if (getTemplatesWaitingForEntry() != null && !getTemplatesWaitingForEntry().isEmpty())
            return new ArrayList<ITemplateHolder>(getTemplatesWaitingForEntry());
        else
            return null;
    }

    //NOTE- call only when entry is locked
    @Override
    public void addTemplateWaitingForEntry(ITemplateHolder template) {
        if (getTemplatesWaitingForEntry() == null)
            initWaitingFor();
        if (!getTemplatesWaitingForEntry().contains(template))
            getTemplatesWaitingForEntry().add(template);
        if (getTemplatesWaitingForEntry().size() == 1 && !hasWaitingFor())
            setHasWaitingFor(true);
    }

    //NOTE- call only when entry is locked
    @Override
    public void removeTemplateWaitingForEntry(ITemplateHolder template) {
        if (getTemplatesWaitingForEntry() != null)
            getTemplatesWaitingForEntry().remove(template);
        if (getTemplatesWaitingForEntry() != null && getTemplatesWaitingForEntry().isEmpty() && hasWaitingFor())
            setHasWaitingFor(false);
    }

    protected IEntryHolder getOtherUpdateUnderXtnEntry() {
        // Get local reference (volatile):
        ITransactionalEntryData entryData = _entryData;
        return entryData.getOtherUpdateUnderXtnEntry();
    }

    @Override
    public boolean hasShadow(boolean safeEntry) {
        boolean hasShadow = !isShadow() && getOtherUpdateUnderXtnEntry() != null;
        return (safeEntry ? hasShadow : hasShadow && isMaybeUnderXtn());
    }

    @Override
    public ShadowEntryHolder getShadow() {
        if (isShadow())
            return (ShadowEntryHolder) this;

        if (hasShadow())
            return (ShadowEntryHolder) getOtherUpdateUnderXtnEntry();

        return null;
    }

    @Override
    public IEntryHolder getMaster() {
        return isShadow() ? getOtherUpdateUnderXtnEntry() : this;
    }

    @Override
    public boolean isExpired(long limit) {
        if (isDummyLeaseAndNotExpired()) {
            return false;
        }
        ITransactionalEntryData ed = _entryData;
        return ed.isExpired(limit);
    }

    @Override
    public boolean isExpired() {
        if (isDummyLeaseAndNotExpired()) {
            return false;
        }
        ITransactionalEntryData ed = _entryData;
        return ed.isExpired();
    }

    @Override
    public boolean isSameEntryInstance(IEntryHolder other) {
        return this == other;
    }

    @Override
    public boolean isBlobStoreEntry() {
        return false;
    }

    @Override
    public IEntryHolder getOriginalEntryHolder() {
        return this;
    }

    //+++++++++++++ ILockObject methods
    @Override
    public ILockObject getExternalLockObject() {
        return null;
    }

    @Override
    public boolean isHollowEntry() {
        return this._entryData == null;
    }
}
