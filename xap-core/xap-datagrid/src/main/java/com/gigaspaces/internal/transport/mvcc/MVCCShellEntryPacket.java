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
package com.gigaspaces.internal.transport.mvcc;

import com.gigaspaces.internal.client.QueryResultTypeInternal;
import com.gigaspaces.internal.client.StorageTypeDeserialization;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.EntryType;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.query.ICustomQuery;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.TransportPacketType;
import com.j_spaces.core.OperationID;
import com.j_spaces.core.cache.mvcc.MVCCShellEntryCacheInfo;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * MVCC shell entry packet, built from {@link com.j_spaces.core.cache.mvcc.MVCCShellEntryCacheInfo},
 * is intended for space recovery with enabled mvcc.
 * EntryPackets list has same ascending order as in the shell.
 * Each entryPacket in the list is built from corresponded eHolder in the shell and has to be instanceof
 * {@link com.gigaspaces.internal.transport.mvcc.IMVCCEntryPacket} (keep mvcc entry metadata)
 *
 * @author Davyd Savitskyi
 * @since 16.4.0
 */
@com.gigaspaces.api.InternalApi
public class MVCCShellEntryPacket implements IEntryPacket {

    private static final long serialVersionUID = 6212108453322058146L;
    private String _uid;
    private String _typeName;
    private List<IMVCCEntryPacket> _entryVersionsPackets;

    public MVCCShellEntryPacket() {
    }

    public MVCCShellEntryPacket(MVCCShellEntryCacheInfo pEntryShell) {
        _entryVersionsPackets = new ArrayList<>(pEntryShell.getTotalCommittedGenertions());
        _uid = pEntryShell.getUID();
        _typeName = pEntryShell.getServerTypeDesc().getTypeName();
    }

    public List<IMVCCEntryPacket> getEntryVersionsPackets() {
        return _entryVersionsPackets;
    }

    public void addEntryVersionPacket(IMVCCEntryPacket packet) {
        _entryVersionsPackets.add(packet);
    }

    @Override
    public void setUID(String uid) {
        this._uid = uid;
    }

    @Override
    public Object getID() {
        return null;
    }

    @Override
    public long getTTL() {
        return 0;
    }

    @Override
    public void setTTL(long ttl) {

    }

    @Override
    public IEntryPacket clone() {
        return null;
    }

    @Override
    public void setCustomQuery(ICustomQuery customQuery) {

    }

    @Override
    public void setPreviousVersion(int version) {

    }

    @Override
    public int getPreviousVersion() {
        return 0;
    }

    @Override
    public boolean hasPreviousVersion() {
        return false;
    }

    @Override
    public boolean hasFixedPropertiesArray() {
        return false;
    }

    @Override
    public boolean isExternalizableEntryPacket() {
        return false;
    }

    @Override
    public OperationID getOperationID() {
        return null;
    }

    @Override
    public void setOperationID(OperationID operationID) {

    }

    @Override
    public EntryType getEntryType() {
        return null;
    }

    @Override
    public TransportPacketType getPacketType() {
        return null;
    }

    @Override
    public String getTypeName() {
        return _typeName;
    }

    @Override
    public String getCodebase() {
        return null;
    }

    @Override
    public ITypeDesc getTypeDescriptor() {
        return null;
    }

    @Override
    public void setTypeDesc(ITypeDesc typeDesc, boolean serializeTypeDesc) {

    }

    @Override
    public int getTypeDescChecksum() {
        return 0;
    }

    @Override
    public boolean supportsTypeDescChecksum() {
        return false;
    }

    @Override
    public boolean isSerializeTypeDesc() {
        return false;
    }

    @Override
    public void setSerializeTypeDesc(boolean serializeTypeDesc) {

    }

    @Override
    public boolean isFifo() {
        return false;
    }

    @Override
    public int getVersion() {
        return 0;
    }

    @Override
    public void setVersion(int version) {

    }

    @Override
    public boolean isNoWriteLease() {
        return false;
    }

    @Override
    public Object[] getFieldValues() {
        return new Object[0];
    }

    @Override
    public void setFieldsValues(Object[] values) {

    }

    @Override
    public Object getFieldValue(int index) {
        return null;
    }

    @Override
    public void setFieldValue(int index, Object value) {

    }

    @Override
    public Object getPropertyValue(String name) {
        return null;
    }

    @Override
    public void setPropertyValue(String name, Object value) {

    }

    @Override
    public Map<String, Object> getDynamicProperties() {
        return null;
    }

    @Override
    public void setDynamicProperties(Map<String, Object> properties) {

    }

    @Override
    public String[] getMultipleUIDs() {
        return new String[0];
    }

    @Override
    public void setMultipleUIDs(String[] uids) {

    }

    @Override
    public String getUID() {
        return _uid;
    }

    @Override
    public Object toObject(QueryResultTypeInternal resultType) {
        return null;
    }

    @Override
    public Object toObject(QueryResultTypeInternal resultType, StorageTypeDeserialization storageTypeDeserialization) {
        return null;
    }

    @Override
    public Object toObject(EntryType entryType) {
        return null;
    }

    @Override
    public Object toObject(EntryType entryType, StorageTypeDeserialization storageTypeDeserialization) {
        return null;
    }

    @Override
    public String getExternalEntryImplClassName() {
        return null;
    }

    @Override
    public boolean isTransient() {
        return false;
    }

    @Override
    public ICustomQuery getCustomQuery() {
        return null;
    }

    @Override
    public Object getRoutingFieldValue() {
        return null;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        serialize(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        deserialize(in);
    }

    @Override
    public void writeToSwap(ObjectOutput out) throws IOException {
        serialize(out);
    }

    @Override
    public void readFromSwap(ObjectInput in) throws IOException, ClassNotFoundException {
        deserialize(in);
    }

    private void deserialize(ObjectInput in) throws IOException, ClassNotFoundException {
        _uid = IOUtils.readString(in);
        _typeName = IOUtils.readString(in);
        _entryVersionsPackets = IOUtils.readList(in);
    }

    private void serialize(ObjectOutput out) throws IOException {
        IOUtils.writeString(out, _uid);
        IOUtils.writeString(out, _typeName);
        IOUtils.writeList(out, _entryVersionsPackets);
    }

    @Override
    public String toString() {
        return "MVCCShellEntryPacket{" +
                "_uid='" + _uid + '\'' +
                ", packets=" + _entryVersionsPackets +
                '}';
    }
}
