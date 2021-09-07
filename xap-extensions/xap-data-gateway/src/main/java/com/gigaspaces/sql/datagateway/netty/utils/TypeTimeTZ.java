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
package com.gigaspaces.sql.datagateway.netty.utils;

import com.gigaspaces.jdbc.calcite.pg.PgTypeDescriptor;
import com.gigaspaces.sql.datagateway.netty.exception.ProtocolException;
import com.gigaspaces.sql.datagateway.netty.query.Session;
import io.netty.buffer.ByteBuf;

import java.time.OffsetTime;

public class TypeTimeTZ extends PgType {
    public static final PgType INSTANCE = new TypeTimeTZ();

    public TypeTimeTZ() {
        super(PgTypeDescriptor.TIME_WITH_TIME_ZONE);
    }

    @Override
    protected void asTextInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
        TypeUtils.checkType(value, OffsetTime.class);
        TypeUtils.writeText(session, dst, session.getDateTimeUtils().toString(value, true));
    }

    @Override @SuppressWarnings("unchecked")
    protected <T> T fromTextInternal(Session session, ByteBuf src) throws ProtocolException {
        return (T) session.getDateTimeUtils().parseOffsetTime(TypeUtils.readText(session, src));
    }

    @Override
    protected void asBinaryInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
        TypeUtils.checkType(value, OffsetTime.class);

        // Binary format is 12 bytes, first 8 bytes is a long, representing a number
        // of microseconds from the start of the day, next 4 bytes is an integer,
        // that represents a zone offset in seconds

        DateTimeUtils utils = session.getDateTimeUtils();
        OffsetTime offsetTime = (OffsetTime) value;
        dst.writeInt(12);
        dst.writeLong(utils.toPgMicros(offsetTime.toLocalTime()));
        dst.writeInt(utils.toPgOffset(offsetTime.getOffset()));
    }

    @Override @SuppressWarnings("unchecked")
    protected <T> T fromBinaryInternal(Session session, ByteBuf src) throws ProtocolException {
        TypeUtils.checkLen(src, 12);
        DateTimeUtils utils = session.getDateTimeUtils();
        return (T) utils.toLocalTime(src.readLong()).atOffset(utils.toJavaOffset(src.readInt()));
    }
}
