package com.gigaspaces.sql.datagateway.netty.utils;

import com.gigaspaces.jdbc.calcite.pg.PgTypeDescriptor;
import com.gigaspaces.sql.datagateway.netty.exception.ProtocolException;
import com.gigaspaces.sql.datagateway.netty.query.Session;
import io.netty.buffer.ByteBuf;

import java.sql.Time;
import java.time.LocalTime;

public class TypeTime extends PgType {
    public static final PgType INSTANCE = new TypeTime();

    public TypeTime() {
        super(PgTypeDescriptor.TIME);
    }

    @Override
    protected void asTextInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
        TypeUtils.checkType(value, Time.class, LocalTime.class);
        TypeUtils.writeText(session, dst, session.getDateTimeUtils().toString(value, false));
    }

    @Override @SuppressWarnings("unchecked")
    protected <T> T fromTextInternal(Session session, ByteBuf src) throws ProtocolException {
        return (T) session.getDateTimeUtils().parseLocalTime(TypeUtils.readText(session, src));
    }

    @Override
    protected void asBinaryInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
        TypeUtils.checkType(value, Time.class, LocalTime.class);

        // Binary format is a long, representing a number
        // of microseconds from the start of the day

        if (value instanceof Time)
            dst.writeInt(8).writeLong(session.getDateTimeUtils().toPgMicros((Time) value));
        else
            dst.writeInt(8).writeLong(session.getDateTimeUtils().toPgMicros((LocalTime) value));
    }

    @Override @SuppressWarnings("unchecked")
    protected <T> T fromBinaryInternal(Session session, ByteBuf src) throws ProtocolException {
        TypeUtils.checkLen(src, 8);
        return (T) session.getDateTimeUtils().toLocalTime(src.readLong());
    }
}
