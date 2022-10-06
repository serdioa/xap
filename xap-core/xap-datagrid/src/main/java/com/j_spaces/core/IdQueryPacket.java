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

package com.j_spaces.core;

import com.gigaspaces.entry.CompoundSpaceId;
import com.gigaspaces.internal.client.QueryResultTypeInternal;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.transport.AbstractProjectionTemplate;
import com.gigaspaces.internal.transport.AbstractQueryPacket;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.metadata.SpaceMetadataException;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Used for querying the space by a class + id.
 *
 * @author Niv Ingberg
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class IdQueryPacket extends AbstractQueryPacket {
    private static final long serialVersionUID = 1L;

    private String _className;
    private Object _id;
    private int _version;
    private int _propertiesLength;
    private Object[] _values;
    private int[] _idFieldIndexes;
    private int _routingFieldIndex;
    private AbstractProjectionTemplate _projectionTemplate;

    private transient String _uid;

    private boolean hasRouting;

    private Object _routing;

    /**
     * Empty constructor required by Externalizable.
     */
    public IdQueryPacket() {
    }

    public IdQueryPacket(Object id, Object routing, int version, ITypeDesc typeDesc, QueryResultTypeInternal resultType, AbstractProjectionTemplate projectionTemplate) {
        super(typeDesc, resultType);
        this._id = id;
        this._version = version;
        this._projectionTemplate = projectionTemplate;
        this._className = typeDesc.getTypeName();
        this._propertiesLength = typeDesc.getNumOfFixedProperties();
        this._idFieldIndexes = typeDesc.getIdentifierPropertiesId();
        this._routingFieldIndex = typeDesc.getRoutingPropertyId();
        this.hasRouting = typeDesc.hasRouting();
        this._routing = routing;
        initValues(routing);
    }

    private void initValues(Object routing) {
        _values = new Object[_propertiesLength];
        if (_id != null) {
            if (_idFieldIndexes.length == 1)
                _values[_idFieldIndexes[0]] = _id;
            else {
                CompoundSpaceId compoundId = assertIsArray(_id, _idFieldIndexes.length);
                for (int i = 0; i < compoundId.length(); i++)
                    _values[_idFieldIndexes[i]] = compoundId.getValue(i);

                if (routing == null && hasRouting) {
                    _routingFieldIndex = -1; //broadcast
                }
            }
        }
        if (routing != null && hasRouting) {
            _values[_routingFieldIndex] = routing;
            _routing = routing;
        } else if (_typeDesc != null) {
            if (!_typeDesc.isRoutingSameAsId()) {
            } else _routing = super.getRoutingFieldValue();
        }
    }

    private CompoundSpaceId assertIsArray(Object obj, int expectedSize) {
        try {
            CompoundSpaceId compoundId = (CompoundSpaceId) obj;
            if (compoundId.length() != expectedSize)
                throw new SpaceMetadataException("Class " + _className + " has compound space id with " + expectedSize +
                        " elements, but provided id value is an array with " + compoundId.length() + " elements");
            return compoundId;
        } catch (ClassCastException e) {
            throw new SpaceMetadataException("Class " + _className + " has compound space id, but provided id value is not a CompoundSpaceId");
        }
    }

    @Override
    public void setUID(String uid) {
        _uid = uid;
    }

    @Override
    public String getUID() {
        return _uid;
    }

    @Override
    public String getTypeName() {
        return _className;
    }

    @Override
    public int getVersion() {
        return this._version;
    }

    @Override
    public Object[] getFieldValues() {
        return _values;
    }

    @Override
    public Object getID() {
        return _id;
    }

    @Override
    public Object getRoutingFieldValue() {
        return _routing;
    }

    @Override
    public AbstractProjectionTemplate getProjectionTemplate() {
        return _projectionTemplate;
    }

    @Override
    public void setProjectionTemplate(AbstractProjectionTemplate projectionTemplate) {
        this._projectionTemplate = projectionTemplate;
    }

    @Override
    public boolean isIdQuery() {
        return true;
    }

    private static final byte HAS_CLASS_NAME = 1 << 0;
    private static final byte HAS_VERSION = 1 << 1;
    private static final byte HAS_PROPERTIES = 1 << 2;
    private static final byte HAS_ID = 1 << 3;
    private static final byte HAS_ROUTING = 1 << 4;
    private static final byte HAS_PROJECTION = 1 << 5;

    @Override
    protected void readExternal(ObjectInput in, PlatformLogicalVersion version) throws IOException, ClassNotFoundException {
        super.readExternal(in, version);

        deserialize(in, version);
    }

    @Override
    public void readFromSwap(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readFromSwap(in);

        deserialize(in, PlatformLogicalVersion.getLogicalVersion());
    }

    private final void deserialize(ObjectInput in, PlatformLogicalVersion version) throws IOException,
            ClassNotFoundException {
        byte flags = in.readByte();

        if ((flags & HAS_CLASS_NAME) != 0) {
            this._className = IOUtils.readRepetitiveString(in);
        }
        if ((flags & HAS_VERSION) != 0)
            this._version = in.readInt();
        if ((flags & HAS_PROPERTIES) != 0)
            this._propertiesLength = in.readInt();

        if ((flags & HAS_ID) != 0) {
            if (version.greaterOrEquals(PlatformLogicalVersion.v16_1_1))
                this._idFieldIndexes = IOUtils.readIntegerArray(in);
            else
                this._idFieldIndexes = new int[]{in.readInt()};
            this._id = IOUtils.readObject(in);
        }
        Object routing = null;
        if ((flags & HAS_ROUTING) != 0) {
            this._routingFieldIndex = in.readInt();
            routing = IOUtils.readObject(in);
        }

        if ((flags & HAS_PROJECTION) != 0)
            this._projectionTemplate = IOUtils.readObject(in);

        initValues(routing);
    }


    @Override
    protected void writeExternal(ObjectOutput out, PlatformLogicalVersion version) throws IOException {
        super.writeExternal(out, version);

        serialize(out, version);
    }

    @Override
    public void writeToSwap(ObjectOutput out) throws IOException {
        super.writeToSwap(out);
        serialize(out, PlatformLogicalVersion.getLogicalVersion());
    }

    private final void serialize(ObjectOutput out, PlatformLogicalVersion version) throws IOException {
        Object routing = getRoutingFieldValue();
        byte flags = buildFlags(routing, version);
        out.writeByte(flags);

        if (_className != null)
            IOUtils.writeRepetitiveString(out, _className);
        if (_version != 0)
            out.writeInt(this._version);
        if (_propertiesLength != 0)
            out.writeInt(_propertiesLength);

        if (_id != null) {
            if (version.greaterOrEquals(PlatformLogicalVersion.v16_1_1))
                IOUtils.writeIntegerArray(out, _idFieldIndexes);
            else
                out.writeInt(this._idFieldIndexes[0]);
            IOUtils.writeObject(out, _id);
        }
        if (routing != null) {
            out.writeInt(this._routingFieldIndex);
            IOUtils.writeObject(out, routing);
        }
        if (_projectionTemplate != null && version.greaterOrEquals(PlatformLogicalVersion.v9_5_0))
            IOUtils.writeObject(out, _projectionTemplate);
    }

    private byte buildFlags(Object routing, PlatformLogicalVersion version) {
        byte flags = 0;

        if (this._className != null)
            flags |= HAS_CLASS_NAME;
        if (this._version != 0)
            flags |= HAS_VERSION;
        if (this._propertiesLength != 0)
            flags |= HAS_PROPERTIES;
        if (_id != null)
            flags |= HAS_ID;
        if (routing != null)
            flags |= HAS_ROUTING;
        if (this._projectionTemplate != null && version.greaterOrEquals(PlatformLogicalVersion.v9_5_0))
            flags |= HAS_PROJECTION;
        return flags;
    }
}
