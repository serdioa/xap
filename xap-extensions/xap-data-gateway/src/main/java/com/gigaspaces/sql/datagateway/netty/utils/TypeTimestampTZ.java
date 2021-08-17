package com.gigaspaces.sql.datagateway.netty.utils;

import com.gigaspaces.jdbc.calcite.pg.PgTypeDescriptor;
import com.gigaspaces.sql.datagateway.netty.exception.ProtocolException;
import com.gigaspaces.sql.datagateway.netty.query.Session;
import io.netty.buffer.ByteBuf;

import java.time.*;
import java.util.Calendar;
import java.util.Date;

public class TypeTimestampTZ extends PgType {
    public static final PgType INSTANCE = new TypeTimestampTZ();

    public TypeTimestampTZ() {
        super(PgTypeDescriptor.TIMESTAMP_WITH_TIME_ZONE);
    }

    @Override
    protected void asTextInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
        TypeUtils.checkType(value,
                Date.class,
                Calendar.class,
                OffsetDateTime.class,
                ZonedDateTime.class,
                Instant.class);
        TypeUtils.writeText(session, dst, session.getDateTimeUtils().toString(value, true));
    }

    @Override @SuppressWarnings("unchecked")
    protected <T> T fromTextInternal(Session session, ByteBuf src) throws ProtocolException {
        return (T) session.getDateTimeUtils().parseInstant(TypeUtils.readText(session, src));
    }

    @Override
    protected void asBinaryInternal(Session session, ByteBuf dst, Object value) throws ProtocolException {
        TypeUtils.checkType(value,
                Date.class,
                Calendar.class,
                OffsetDateTime.class,
                ZonedDateTime.class,
                Instant.class);

        // Binary format is 12 bytes, first 8 bytes is a long, representing a number
        // of microseconds from the postgres epoch (2000-01-01), next 4 bytes is an integer,
        // that represents a zone offset in seconds

        DateTimeUtils utils = session.getDateTimeUtils();

        if (value instanceof Date) {
            dst.writeInt(12);
            dst.writeLong(utils.toPgMicros((Date) value));
            dst.writeInt(utils.toPgOffset(ZoneOffset.UTC));
        } else if (value instanceof Calendar) {
            dst.writeInt(12);
            dst.writeLong(utils.toPgMicros((Calendar) value));
            dst.writeInt(utils.getPgOffset((Calendar)value));
        } else if (value instanceof OffsetDateTime) {
            OffsetDateTime offsetDateTime = (OffsetDateTime) value;
            dst.writeInt(12);
            dst.writeLong(utils.toPgMicros(offsetDateTime.toLocalDateTime()));
            dst.writeInt(utils.toPgOffset(offsetDateTime.getOffset()));
        } else if (value instanceof ZonedDateTime) {
            ZonedDateTime zonedDateTime = (ZonedDateTime) value;
            dst.writeInt(12);
            dst.writeLong(utils.toPgMicros(zonedDateTime.toLocalDateTime()));
            dst.writeInt(utils.toPgOffset(zonedDateTime.getOffset()));
        } else {
            dst.writeInt(12);
            dst.writeLong(utils.toPgMicros((Instant) value));
            dst.writeInt(utils.toPgOffset(ZoneOffset.UTC));
        }
    }

    @Override @SuppressWarnings("unchecked")
    protected <T> T fromBinaryInternal(Session session, ByteBuf src) throws ProtocolException {
        TypeUtils.checkLen(src, 12);
        DateTimeUtils utils = session.getDateTimeUtils();
        LocalDateTime localDateTime = utils.toLocalDateTime(src.readLong());
        ZoneOffset zoneOffset = utils.toJavaOffset(src.readInt());
        return (T) localDateTime.atOffset(zoneOffset);
    }
}
