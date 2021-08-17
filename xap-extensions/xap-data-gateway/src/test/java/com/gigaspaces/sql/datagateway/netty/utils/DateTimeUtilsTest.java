package com.gigaspaces.sql.datagateway.netty.utils;

import org.junit.jupiter.api.Test;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.*;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.*;

class DateTimeUtilsTest {
    private final DateTimeUtils utils = new DateTimeUtils(TimeZone::getDefault);

    @Test
    public void testLocalDate() throws Exception {
        LocalDate orig = LocalDate.parse("2007-12-03");
        LocalDate parsed = utils.parseLocalDate(utils.toString(orig));
        assertEquals(orig, parsed);
    }

    @Test
    public void testLocalTime() throws Exception {
        LocalTime orig = LocalTime.parse("10:15:30.5456");
        LocalTime parsed = utils.parseLocalTime(utils.toString(orig));
        assertEquals(orig, parsed);
    }

    @Test
    public void testLocalDateTime() throws Exception {
        LocalDateTime orig = LocalDateTime.parse("2007-12-03T10:15:30.5456");
        LocalDateTime parsed = utils.parseLocalDateTime(utils.toString(orig));
        assertEquals(orig, parsed);
    }

    @Test
    public void testTimestamp() throws Exception {
        LocalDateTime orig = LocalDateTime.parse("2007-12-03T10:15:30.5456");
        LocalDateTime parsed = utils.parseLocalDateTime(utils.toString(Timestamp.valueOf(orig)));
        assertEquals(orig, parsed);
    }

    @Test
    public void testTime() throws Exception {
        LocalTime orig = LocalTime.parse("10:15:30");
        LocalTime parsed = utils.parseLocalTime(utils.toString(Time.valueOf(orig)));
        assertEquals(orig, parsed);
    }

    @Test
    public void testDate() throws Exception {
        LocalDate orig = LocalDate.parse("2007-12-03");
        LocalDate parsed = utils.parseLocalDate(utils.toString(Date.valueOf(orig)));
        assertEquals(orig, parsed);
    }

    @Test
    public void testOffsetTime() throws Exception {
        ZoneOffset offset = ZoneOffset.of("+08:30");
        OffsetTime orig = LocalTime.parse("10:15:30").atOffset(offset);
        OffsetTime parsed = utils.parseOffsetTime(utils.toString(orig));
        assertEquals(orig, parsed);
    }

    @Test
    public void testOffsetDateTime() throws Exception {
        ZoneOffset offset = ZoneOffset.of("+08:30");
        OffsetDateTime orig = LocalDateTime.parse("2007-12-03T10:15:30").atOffset(offset);
        OffsetDateTime parsed = utils.parseOffsetDateTime(utils.toString(orig));
        assertEquals(orig, parsed);
    }

    @Test
    public void testLocalDateBinary() {
        LocalDate orig = LocalDate.parse("2007-12-03");
        LocalDate parsed = utils.toLocalDate(utils.toPgMicros(orig));
        assertEquals(orig, parsed);
    }

    @Test
    public void testLocalTimeBinary() {
        LocalTime orig = LocalTime.parse("10:15:30.5456");
        LocalTime parsed = utils.toLocalTime(utils.toPgMicros(orig));
        assertEquals(orig, parsed);
    }

    @Test
    public void testLocalDateTimeBinary() {
        LocalDateTime orig = LocalDateTime.parse("2007-12-03T10:15:30.5456");
        LocalDateTime parsed = utils.toLocalDateTime(utils.toPgMicros(orig));
        assertEquals(orig, parsed);
    }

    @Test
    public void testTimestampBinary() {
        LocalDateTime orig = LocalDateTime.parse("2007-12-03T10:15:30");
        LocalDateTime parsed = utils.toLocalDateTime(utils.toPgMicros(Timestamp.valueOf(orig)));
        assertEquals(orig, parsed);
    }

    @Test
    public void testTimeBinary() {
        LocalTime orig = LocalTime.parse("10:15:30");
        LocalTime parsed = utils.toLocalTime(utils.toPgMicros(Time.valueOf(orig)));
        assertEquals(orig, parsed);
    }

    @Test
    public void testDateBinary() {
        LocalDate orig = LocalDate.parse("2000-12-01");
        LocalDate parsed = utils.toLocalDate(utils.toPgMicros(Date.valueOf(orig)));
        assertEquals(orig, parsed);
    }

    @Test
    public void testJavaDateBinary() throws Exception {
        SimpleDateFormat format = new SimpleDateFormat("yyy-MM-dd");
        java.util.Date orig = format.parse("2000-12-01");
        LocalDateTime parsed = utils.toLocalDateTime(utils.toPgMicros(orig));
        assertEquals(orig.toInstant(), parsed.toInstant(ZoneOffset.UTC));
    }

    @Test
    public void testOffsetTimeBinary() {
        ZoneOffset offset = ZoneOffset.of("+08:30");
        OffsetTime orig = LocalTime.parse("10:15:30").atOffset(offset);
        long pgMicros = utils.toPgMicros(orig.toLocalTime());
        int pgOffset = utils.toPgOffset(orig.getOffset());
        OffsetTime parsed = utils.toLocalTime(pgMicros).atOffset(utils.toJavaOffset(pgOffset));
        assertEquals(orig, parsed);
    }

    @Test
    public void testOffsetDateTimeBinary() {
        ZoneOffset offset = ZoneOffset.of("+08:30");
        OffsetDateTime orig = LocalDateTime.parse("2007-12-03T10:15:30").atOffset(offset);
        long pgMicros = utils.toPgMicros(orig.toLocalDateTime());
        int pgOffset = utils.toPgOffset(orig.getOffset());
        ZoneOffset offset0 = utils.toJavaOffset(pgOffset);
        LocalDateTime localDateTime = utils.toLocalDateTime(pgMicros);
        OffsetDateTime parsed = localDateTime.atOffset(offset0);
        assertEquals(orig, parsed);
    }
}