package com.gigaspaces.sql.datagateway.netty.utils;

import com.gigaspaces.jdbc.calcite.pg.PgTypeDescriptor;
import com.gigaspaces.sql.datagateway.netty.exception.ProtocolException;
import com.gigaspaces.sql.datagateway.netty.query.Session;
import io.netty.buffer.ByteBuf;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Date;

public class TypeTimestamp extends PgType {
    public static final PgType INSTANCE = new TypeTimestamp();

    public TypeTimestamp() {
        super(PgTypeDescriptor.TIMESTAMP);
    }

    @Override
    protected void asTextInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
        TypeUtils.checkType(value, Timestamp.class, Date.class, LocalDateTime.class);
        TypeUtils.writeText(session, dst, session.getDateTimeUtils().toString(value, false));
    }

    @Override @SuppressWarnings("unchecked")
    protected <T> T fromTextInternal(Session session, ByteBuf src) throws ProtocolException {
        return (T) session.getDateTimeUtils().parseLocalDateTime(TypeUtils.readText(session, src));
    }

    @Override
    protected void asBinaryInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
        TypeUtils.checkType(value, Timestamp.class, Date.class, LocalDateTime.class);

        // Binary format is a long, representing a number
        // of microseconds from the postgres epoch (2000-01-01)

        if (value instanceof Timestamp)
            dst.writeInt(8).writeLong(session.getDateTimeUtils().toPgMicros((Timestamp) value));
        if (value instanceof Date)
            dst.writeInt(8).writeLong(session.getDateTimeUtils().toPgMicros((Date) value));
        else
            dst.writeInt(8).writeLong(session.getDateTimeUtils().toPgMicros((LocalDateTime) value));
    }

    @Override @SuppressWarnings("unchecked")
    protected <T> T fromBinaryInternal(Session session, ByteBuf src) throws ProtocolException {
        TypeUtils.checkLen(src, 8);
        return (T) session.getDateTimeUtils().toLocalDateTime(src.readLong());
    }
}
