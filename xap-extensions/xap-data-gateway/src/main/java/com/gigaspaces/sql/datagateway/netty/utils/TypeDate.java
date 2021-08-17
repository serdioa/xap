package com.gigaspaces.sql.datagateway.netty.utils;

import com.gigaspaces.jdbc.calcite.pg.PgTypeDescriptor;
import com.gigaspaces.sql.datagateway.netty.exception.ProtocolException;
import com.gigaspaces.sql.datagateway.netty.query.Session;
import io.netty.buffer.ByteBuf;

import java.sql.Date;
import java.time.LocalDate;

public class TypeDate extends PgType {
    public static final PgType INSTANCE = new TypeDate();

    public TypeDate() {
        super(PgTypeDescriptor.DATE);
    }

    @Override
    protected void asTextInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
        TypeUtils.checkType(value, Date.class, LocalDate.class);
        TypeUtils.writeText(session, dst, session.getDateTimeUtils().toString(value, false));
    }

    @Override @SuppressWarnings("unchecked")
    protected <T> T fromTextInternal(Session session, ByteBuf src) throws ProtocolException {
        return (T) session.getDateTimeUtils().parseLocalDate(TypeUtils.readText(session, src));
    }

    @Override
    protected void asBinaryInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
        TypeUtils.checkType(value, Date.class, LocalDate.class);

        // Binary format is an integer, representing a number
        // of days from the postgres epoch (2000-01-01)

        if (value instanceof Date)
            dst.writeInt(4).writeInt(session.getDateTimeUtils().toPgMicros((Date) value));
        else
            dst.writeInt(4).writeInt(session.getDateTimeUtils().toPgMicros((LocalDate) value));
    }

    @Override @SuppressWarnings("unchecked")
    protected <T> T fromBinaryInternal(Session session, ByteBuf src) throws ProtocolException {
        TypeUtils.checkLen(src, 4);
        return (T) session.getDateTimeUtils().toLocalDate(src.readInt());
    }
}
